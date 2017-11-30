/*
 * Copyright 2013 LinkedIn Corp
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.execapp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import azkaban.event.Event;
import azkaban.event.Event.Type;
import azkaban.event.EventData;
import azkaban.event.EventHandler;
import azkaban.event.EventListener;
import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.event.JobCallbackManager;
import azkaban.execapp.jmx.JmxJobMBeanManager;
import azkaban.execapp.metric.NumFailedJobMetric;
import azkaban.execapp.metric.NumRunningJobMetric;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.flow.FlowProps;
import azkaban.jobExecutor.ProcessJob;
import azkaban.jobtype.JobTypeManager;
import azkaban.metric.MetricReportManager;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.SwapQueue;

/**
 * Class that handles the running of a ExecutableFlow DAG
 *
 */
public class FlowRunner extends EventHandler implements Runnable {
  private static final Layout DEFAULT_LAYOUT = new PatternLayout(
      "%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n");

  // We check update every 5 minutes, just in case things get stuck. But for the
  // most part, we'll be idling.
  private static final long CHECK_WAIT_MS = 5 * 60 * 1000;

  private Logger logger;
  private Layout loggerLayout = DEFAULT_LAYOUT;
  private Appender flowAppender;
  private File logFile;

  private ExecutorService executorService;    // flow 执行 job 的线程
  private ExecutorLoader executorLoader;      // executor 加载器
  private ProjectLoader projectLoader;        // project  加载器

  private int execId;                         // 执行的 execId
  private File execDir;                       // 执行的目录
  private final ExecutableFlow flow;          // flow 信息
  private Thread flowRunnerThread;            // flow 的执行线程
  private int numJobThreads = 10;
  private ExecutionOptions.FailureAction failureAction;  // 失败动作

  // Sync object for queuing
  private Object mainSyncObj = new Object();    // 用于线程同步的类

  // Properties map
  private Map<String, Props> sharedProps = new HashMap<String, Props>();
  private final JobTypeManager jobtypeManager;

  // job 运行的监听器
  private JobRunnerEventListener listener = new JobRunnerEventListener();

  // ???
  private Set<JobRunner> activeJobRunners = Collections
      .newSetFromMap(new ConcurrentHashMap<JobRunner, Boolean>());

  // ????
  // Thread safe swap queue for finishedExecutions.
  private SwapQueue<ExecutableNode> finishedNodes;

  // Used for pipelining
  private Integer pipelineLevel = null;
  private Integer pipelineExecId = null;

  // Watches external flows for execution.
  private FlowWatcher watcher = null;
  private String flowOwnUser = null;
  private String flowOwnGroup = null;
  private String flowLastModifyUser = null;
  private Set<String> proxyUsers = null;
  private boolean validateUserProxy;

  private String jobLogFileSize = "5MB";
  private int jobLogNumFiles = 4;

  private boolean flowPaused = false;
  private boolean flowFailed = false;
  private boolean flowFinished = false;
  private boolean flowKilled = false;
  // The following is state that will trigger a retry of all failed jobs
  private boolean retryFailedJobs = false;

  /**
   * Constructor. This will create its own ExecutorService for thread pools
   *
   * @param flow
   * @param executorLoader
   * @param projectLoader
   * @param jobtypeManager
   * @throws ExecutorManagerException
   */
    public FlowRunner(ExecutableFlow flow, ExecutorLoader executorLoader,
      ProjectLoader projectLoader, JobTypeManager jobtypeManager)
      throws ExecutorManagerException {
    this(flow, executorLoader, projectLoader, jobtypeManager, null);
  }

  /**
   * Constructor. If executorService is null, then it will create it's own for
   * thread pools.
   *
   * @param flow
   * @param executorLoader
   * @param projectLoader
   * @param jobtypeManager
   * @param executorService
   * @throws ExecutorManagerException
   */
  public FlowRunner(ExecutableFlow flow, ExecutorLoader executorLoader,
      ProjectLoader projectLoader, JobTypeManager jobtypeManager,
      ExecutorService executorService) throws ExecutorManagerException {

    this.execId = flow.getExecutionId();
    this.flow = flow;
    this.executorLoader = executorLoader;
    this.projectLoader = projectLoader;
    this.execDir = new File(flow.getExecutionPath());
    this.jobtypeManager = jobtypeManager;

    this.flowOwnGroup = flow.getFlowOwnGroup();
    this.flowLastModifyUser = flow.getLastModifiedByUser();
    this.flowOwnUser = flow.getFlowOwnUser();


    ExecutionOptions options = flow.getExecutionOptions();
    this.pipelineLevel = options.getPipelineLevel();
    this. pipelineExecId = options.getPipelineExecutionId();
    this.failureAction = options.getFailureAction();
    this.proxyUsers = flow.getProxyUsers();
    this.executorService = executorService;
    this.finishedNodes = new SwapQueue<ExecutableNode>();
  }

  // 如果存在队列中没有执行完的 flow ， 则设置监听等待其结束
  public FlowRunner setFlowWatcher(FlowWatcher watcher) {
    this.watcher = watcher;
    return this;
  }

  // 设置 job 的线程数量
  public FlowRunner setNumJobThreads(int jobs) {
    numJobThreads = jobs;
    return this;
  }

   // 设置 log 的文件
  public FlowRunner setJobLogSettings(String jobLogFileSize, int jobLogNumFiles) {
    this.jobLogFileSize = jobLogFileSize;
    this.jobLogNumFiles = jobLogNumFiles;

    return this;
  }

  // 设置 是否启用用户代理
  public FlowRunner setValidateProxyUser(boolean validateUserProxy) {
    this.validateUserProxy = validateUserProxy;
    return this;
  }

  // 设置用户的执行目录
  public File getExecutionDir() {
    return execDir;
  }

  public void run() {

    try {
      if (this.executorService == null) {
        this.executorService = Executors.newFixedThreadPool(numJobThreads);
      }


      // 准备 Flow 的执行环境
      // 获取线程名字，获取 flow 的执行参数
      setupFlowExecution();
      flow.setStartTime(System.currentTimeMillis());
      logger.info(">>> submit flow : " + flow.getFlowOwnUser() + "[" + flow.getFlowOwnGroup() + "]");
      // active_executing_flows 中记录当前正在执行的 workFlow .
      // 这里刷新下更新时间
      updateFlowReference();

      logger.info("Updating initial flow directory.");
      // 刷新 execution_flows 中关于 flow 的信息
      // status : PREPARING
      updateFlow();
      logger.info("Fetching job and shared properties.");

      // 加载所有的变量属性
      loadAllProperties();

      //System.out.println("2>>>>> flow " + flow.getId() + " status : " + flow.getStatus());

      //  ??? 事件监控的类型
      this.fireEventListeners(Event.create(this, Type.FLOW_STARTED, new EventData(this.getExecutableFlow().getStatus())));

      /*
      *   flow 的执行过程 :
      *           首先是将最开始的 node 启动， 然后轮询等待 node 的结束状态
      *           如果在轮询过程中没有 node 结束， 那么轮询过程将等待 5 分钟后继续执行， 中间如果存在 node 运行完成。则中断等待过程。
      *           当存在 node 结束时候， 将判断是否存在 node 的子节点,如果有则将自节点加入到检查运行队列（依赖检查放在执行时）。
      *           如果这个已经结束的节点状态为失败，则判断是否还有能够重跑的机会， 如果有，则状态重置 ready 并放置到检查队列去下次重新执行。
       */

      runFlow();
    } catch (Throwable t) {
      if (logger != null) {
        logger
            .error(
                "An error has occurred during the running of the flow. Quiting.",
                t);
      }
      flow.setStatus(Status.FAILED);  // 如果有错误将设置 flow 的状态为 flath .
    } finally {
      if (watcher != null) {

        watcher.stopWatcher();
        logger
            .info("Watcher cancelled status is " + watcher.isWatchCancelled());
      }

      flow.setEndTime(System.currentTimeMillis());
      logger.info("Setting end time for flow " + execId + " to "
          + System.currentTimeMillis());
      closeLogger();

      updateFlow();
      this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED, new EventData(flow.getStatus())));
    }
  }

  @SuppressWarnings("unchecked")
  private void setupFlowExecution() {
    int projectId = flow.getProjectId();
    int version = flow.getVersion();
    String flowId = flow.getFlowId();

    // Add a bunch of common azkaban properties
    Props commonFlowProps = PropsUtils.addCommonFlowProperties(null, flow);

    if (flow.getJobSource() != null) {
      String source = flow.getJobSource();
      Props flowProps = sharedProps.get(source);
      flowProps.setParent(commonFlowProps);
      commonFlowProps = flowProps;
    }

    // If there are flow overrides, we apply them now.
    /*
    * 如果用户存在输入的自定义的参数， 则将用户的入参 push 到环境变量中去。
     */
    // 获取用户输入的参数
    Map<String, String> flowParam =
        flow.getExecutionOptions().getFlowParameters();

    /*
    *  添加 azkaban 配置 -- 如果 flow 具有用户组
     */
    if(flow.getFlowOwnGroup() != null){
      // flowParam.putAll(flowProperties.getGroupPropertis(flowOwnGroup));
    }
    if (flowParam != null && !flowParam.isEmpty()) {
      commonFlowProps = new Props(commonFlowProps, flowParam);
    }

    flow.setInputProps(commonFlowProps);

    // Create execution dir
    createLogger(flowId);

    if (this.watcher != null) {
      this.watcher.setLogger(logger);
    }

    logger.info("Assigned executor : " + AzkabanExecutorServer.getApp().getExecutorHostPort());
    logger.info("Running execid:" + execId + " flow:" + flowId + " project:"
        + projectId + " version:" + version);


    if (pipelineExecId != null) {
      logger.info("Running simulateously with " + pipelineExecId
          + ". Pipelining level " + pipelineLevel);
    }

    // The current thread is used for interrupting blocks
    // 设置 flow 的执行线程属性
    flowRunnerThread = Thread.currentThread();
    flowRunnerThread.setName("FlowRunner-exec-" + flow.getExecutionId());
  }

  private void updateFlowReference() throws ExecutorManagerException {
    logger.info("Update active reference");
    if (!executorLoader.updateExecutableReference(execId,
        System.currentTimeMillis())) {
      throw new ExecutorManagerException(
          "The executor reference doesn't exist. May have been killed prematurely.");
    }
  }

  private void updateFlow() {
    updateFlow(System.currentTimeMillis());
  }

  private synchronized void updateFlow(long time) {
    try {
      flow.setUpdateTime(time);
      executorLoader.updateExecutableFlow(flow);
    } catch (ExecutorManagerException e) {
      logger.error("Error updating flow.", e);
    }
  }

  private void createLogger(String flowId) {
    // Create logger
    String loggerName = execId + "." + flowId;
    logger = Logger.getLogger(loggerName);

    // Create file appender
    String logName = "_flow." + loggerName + ".log";
    logFile = new File(execDir, logName);
    String absolutePath = logFile.getAbsolutePath();

    flowAppender = null;
    try {
      flowAppender = new FileAppender(loggerLayout, absolutePath, false);
      logger.addAppender(flowAppender);
    } catch (IOException e) {
      logger.error("Could not open log file in " + execDir, e);
    }
  }

  private void closeLogger() {
    if (logger != null) {
      logger.removeAppender(flowAppender);
      flowAppender.close();

      try {
        executorLoader.uploadLogFile(execId, "", 0, logFile);
      } catch (ExecutorManagerException e) {
        e.printStackTrace();
      }
    }
  }

  private void loadAllProperties() throws IOException {
    // First load all the properties
    for (FlowProps fprops : flow.getFlowProps()) {
      String source = fprops.getSource();
      File propsPath = new File(execDir, source);
      Props props = new Props(null, propsPath);
      sharedProps.put(source, props);
    }

    // Resolve parents
    for (FlowProps fprops : flow.getFlowProps()) {
      if (fprops.getInheritedSource() != null) {
        String source = fprops.getSource();
        String inherit = fprops.getInheritedSource();

        Props props = sharedProps.get(source);
        Props inherits = sharedProps.get(inherit);

        props.setParent(inherits);
      }
    }
  }

  /**
   * Main method that executes the jobs.
   *
   * @throws Exception
   */
  private void runFlow() throws Exception {
    logger.info("Starting flows");
    // 运行 flow 的处于根节点上的 node
    runReadyJob(this.flow);
    // 更新 flow 的状态
    updateFlow();

    // 如果 flow 没有 停止
    while (!flowFinished) {
      synchronized (mainSyncObj) {

        if (flowPaused) {  // 如果 flow 处于暂停状态
          try {
            mainSyncObj.wait(CHECK_WAIT_MS);
          } catch (InterruptedException e) {
          }

          continue;
        } else {
          if (retryFailedJobs) {
            retryAllFailures();
          } else if (!progressGraph()) {
            /*
            *  轮询刷新 node 节点, 并监控获取 node 的信息
            *  看是否有已经结束的 node 节点， 如果有已经结束的 node 节点， 如果 node 失败，则尝试重新执行。
            *  否则，就判断其子节点是否可以执行。如果轮询过程中没有可刷新的状态， 则等待一段时间后结束运行。
            *
            *  等 5 分钟后继续执行或者等待中间 node 执行完触发中断。
             */
            try {
              mainSyncObj.wait(CHECK_WAIT_MS);
            } catch (InterruptedException e) {
            }
          }
        }
      }
    }

    logger.info("Finishing up flow. Awaiting Termination");
    executorService.shutdown();

    updateFlow();
    logger.info("Finished Flow");
  }

  private void retryAllFailures() throws IOException {
    logger.info("Restarting all failed jobs");

    this.retryFailedJobs = false;
    this.flowKilled = false;
    this.flowFailed = false;
    this.flow.setStatus(Status.RUNNING);

    ArrayList<ExecutableNode> retryJobs = new ArrayList<ExecutableNode>();
    resetFailedState(this.flow, retryJobs);

    for (ExecutableNode node : retryJobs) {
      if (node.getStatus() == Status.READY
          || node.getStatus() == Status.DISABLED) {
        runReadyJob(node);
      } else if (node.getStatus() == Status.SUCCEEDED) {
        for (String outNodeId : node.getOutNodes()) {
          ExecutableFlowBase base = node.getParentFlow();
          runReadyJob(base.getExecutableNode(outNodeId));
        }
      }

      runReadyJob(node);
    }

    updateFlow();
  }

  private boolean progressGraph() throws IOException {
    finishedNodes.swap();

    // The following nodes are finished, so we'll collect a list of outnodes
    // that are candidates for running next.
    HashSet<ExecutableNode> nodesToCheck = new HashSet<ExecutableNode>();
    // 遍历已经结束的 node
    for (ExecutableNode node : finishedNodes) {
      Set<String> outNodeIds = node.getOutNodes();
      ExecutableFlowBase parentFlow = node.getParentFlow();

      // If a job is seen as failed, then we set the parent flow to
      // FAILED_FINISHING
      if (node.getStatus() == Status.FAILED) {  // 如果 node 是已经 Failed 掉
        // The job cannot be retried or has run out of retry attempts. We will
        // fail the job and its flow now.
        // 如果不存在 node 的重新调用， 则设置 flow 的当前的状态为 Status.FAILED_FINISHING
        // 如果存在重试需求，则重试重新设置 node 的状态为 ready
        if (!retryJobIfPossible(node)) {
          propagateStatus(node.getParentFlow(), Status.FAILED_FINISHING);
          if (failureAction == FailureAction.CANCEL_ALL) {  // 判断下失败的处理情况
            this.kill();
          }
          this.flowFailed = true;
        } else {
          nodesToCheck.add(node);   // 将这个 node 放入检查队列中。
          continue;
        }
      }

      // 如果 发现 node 没有 子 node , 则 表示 flow 已经结束.
      // 这个地方最终的 node 只有一个。
      if (outNodeIds.isEmpty()) {
        // There's no outnodes means it's the end of a flow, so we finalize
        // and fire an event.
        finalizeFlow(parentFlow);
        finishExecutableNode(parentFlow);

        // If the parent has a parent, then we process
        if (!(parentFlow instanceof ExecutableFlow)) {
          outNodeIds = parentFlow.getOutNodes();
          parentFlow = parentFlow.getParentFlow();
        }
      }

      // Add all out nodes from the finished job. We'll check against this set
      // to
      // see if any are candidates for running.
      for (String nodeId : outNodeIds) {
        ExecutableNode outNode = parentFlow.getExecutableNode(nodeId);
        nodesToCheck.add(outNode);
      }
    }

    // Runs candidate jobs. The code will check to see if they are ready to run
    // before
    // Instant kill or skip if necessary.
    // 检查的 node  包括之前运行中失败的node 跟下次准备要执行的node
    boolean jobsRun = false;
    for (ExecutableNode node : nodesToCheck) {
      //logger.info("2 --- node " + node.getId() + " status : " + node.getStatus());
      // 判断是否已经结束或者正在运行
      if (Status.isStatusFinished(node.getStatus())
          || Status.isStatusRunning(node.getStatus())) {
        // Really shouldn't get in here.
        continue;
      }

      // 在这个地方对 node 检查， 如果依赖没有执行完 则返回 false
      // 如果有需要执行的 则刷新状态，可能 running , cancle,
      jobsRun |= runReadyJob(node);
    }

    // 刷新 flow 的状态, 在运行 job 的时候，跟 有job 结束的时候。
    if (jobsRun || finishedNodes.getSize() > 0) {
      updateFlow();
      return true;
    }

    return false;
  }

  private boolean runReadyJob(ExecutableNode node) throws IOException {

    // 首先检查这个 node 是否运行结束
    if (Status.isStatusFinished(node.getStatus())
        || Status.isStatusRunning(node.getStatus())) {
      return false;
    }

    // 检查判断这个 node 的状态 -- cancel , skip , null , ready.
    Status nextNodeStatus = getImpliedStatus(node);
    if (nextNodeStatus == null) {
      return false;
    }


    if (nextNodeStatus == Status.CANCELLED) {       // node 应该被取消执行。
      logger.info("Cancelling '" + node.getNestedId()
          + "' due to prior errors.");
      node.cancelNode(System.currentTimeMillis());
      finishExecutableNode(node);
    } else if (nextNodeStatus == Status.SKIPPED) {   // node 应该被 skip
      logger.info("Skipping disabled job '" + node.getId() + "'.");
      node.skipNode(System.currentTimeMillis());
      finishExecutableNode(node);
    } else if (nextNodeStatus == Status.READY) {     // node 应该被执行
      if (node instanceof ExecutableFlowBase) {       // node 是 flow Base
        ExecutableFlowBase flow = ((ExecutableFlowBase) node);
        logger.info("Running flow '" + flow.getNestedId() + "'.");
        flow.setStatus(Status.RUNNING);
        flow.setStartTime(System.currentTimeMillis());
        prepareJobProperties(flow);                   // 设置 flow 的准备环境

        for (String startNodeId : ((ExecutableFlowBase) node).getStartNodes()) {
          ExecutableNode startNode = flow.getExecutableNode(startNodeId);
          runReadyJob(startNode);  // 运行所有最初的开始节点
        }
      } else {
        runExecutableNode(node);
      }
    }
    return true;
  }

  private boolean retryJobIfPossible(ExecutableNode node) throws IOException {

    if (node instanceof ExecutableFlowBase) {
      return false;
    }

    // 如果 node 的尝试次数没有超过 node 允许尝试次数
    //node.setAttempt(node.getAttempt()+1);

    try{
      executorLoader.updateExecutableNode(node);
    } catch (ExecutorManagerException e) {
      throw new IOException(e);
    }

    if (node.getRetries() > node.getAttempt() + 1) {
      logger.info("Job '" + node.getId() + "' will be retried. Attempt "
          + (node.getAttempt() +1  ) + " of " + node.getRetries());
      node.setDelayedExecution(node.getRetryBackoff());  // 设置推迟执行时间
      node.resetForRetry();  //  增加尝试次数
      return true;
    } else {
      if (node.getRetries() > 0) {
        logger.info("Job '" + node.getId() + "' has run out of retry attempts");
        // Setting delayed execution to 0 in case this is manually re-tried.
        node.setDelayedExecution(0);
      }

      return false;
    }
  }

  private void propagateStatus(ExecutableFlowBase base, Status status) {
    if (!Status.isStatusFinished(base.getStatus())) {
      logger.info("Setting " + base.getNestedId() + " to " + status);
      base.setStatus(status);
      if (base.getParentFlow() != null) {
        propagateStatus(base.getParentFlow(), status);
      }
    }
  }

  private void finishExecutableNode(ExecutableNode node) {
    finishedNodes.add(node);
    EventData eventData = new EventData(node.getStatus(), node.getNestedId());
    fireEventListeners(Event.create(this, Type.JOB_FINISHED, eventData));
  }

  private void finalizeFlow(ExecutableFlowBase flow) {
    String id = flow == this.flow ? "" : flow.getNestedId();

    // If it's not the starting flow, we'll create set of output props
    // for the finished flow.
    boolean succeeded = true;
    Props previousOutput = null;

    for (String end : flow.getEndNodes()) {
      ExecutableNode node = flow.getExecutableNode(end);

      // 表示 flow 已经失败
      if (node.getStatus() == Status.KILLED
          || node.getStatus() == Status.FAILED
          || node.getStatus() == Status.CANCELLED) {
        succeeded = false;
      }

      Props output = node.getOutputProps();
      if (output != null) {
        output = Props.clone(output);
        output.setParent(previousOutput);
        previousOutput = output;
      }
    }

    flow.setOutputProps(previousOutput);
    // 如果 flow 正在运行，并且 最后停止的 node 没有成功
    if (!succeeded && (flow.getStatus() == Status.RUNNING)) {
      flow.setStatus(Status.KILLED);
    }

    flow.setEndTime(System.currentTimeMillis());
    flow.setUpdateTime(System.currentTimeMillis());
    long durationSec = (flow.getEndTime() - flow.getStartTime()) / 1000;
    switch (flow.getStatus()) {
    case FAILED_FINISHING:
      logger.info("Setting flow '" + id + "' status to FAILED in "
          + durationSec + " seconds");
      flow.setStatus(Status.FAILED);
      break;
    case FAILED:
    case KILLED:
    case CANCELLED:
    case FAILED_SUCCEEDED:
      logger.info("Flow '" + id + "' is set to " + flow.getStatus().toString()
          + " in " + durationSec + " seconds");
      break;
    default:
      flow.setStatus(Status.SUCCEEDED);
      logger.info("Flow '" + id + "' is set to " + flow.getStatus().toString()
          + " in " + durationSec + " seconds");
    }

    // If the finalized flow is actually the top level flow, than we finish
    // the main loop.
    if (flow instanceof ExecutableFlow) {
      flowFinished = true;
    }
  }

  private void prepareJobProperties(ExecutableNode node) throws IOException {
    if (node instanceof ExecutableFlow) {
      return;
    }

    Props props = null;
    // 1. Shared properties (i.e. *.properties) for the jobs only. This takes
    // the
    // least precedence
    if (!(node instanceof ExecutableFlowBase)) {
      String sharedProps = node.getPropsSource();
      if (sharedProps != null) {
        props = this.sharedProps.get(sharedProps);
      }
    }

    // The following is the hiearchical ordering of dependency resolution
    // 2. Parent Flow Properties
    ExecutableFlowBase parentFlow = node.getParentFlow();
    if (parentFlow != null) {
      Props flowProps = Props.clone(parentFlow.getInputProps());   // 从 flow 中获取到 props
      flowProps.setEarliestAncestor(props);
      props = flowProps;
    }

    // 3. Output Properties. The call creates a clone, so we can overwrite it.
    Props outputProps = collectOutputProps(node);
    if (outputProps != null) {
      outputProps.setEarliestAncestor(props);
      props = outputProps;
    }

    // 4. The job source.
    Props jobSource = loadJobProps(node);
    if (jobSource != null) {
      jobSource.setParent(props);
      props = jobSource;
    }
    // 将 flow 的信息放置到 job中
    node.setInputProps(props);
  }

  /**
   * @param props
   * This method is to put in any job properties customization before feeding
   * to the job.
   */
  private void customizeJobProperties(Props props) {
    boolean memoryCheck = flow.getExecutionOptions().getMemoryCheck();
    props.put(ProcessJob.AZKABAN_MEMORY_CHECK, Boolean.toString(memoryCheck));
  }

  private Props loadJobProps(ExecutableNode node) throws IOException {
    Props props = null;
    String source = node.getJobSource();
    if (source == null) {
      return null;
    }

    // load the override props if any
    try {
      props =
          projectLoader.fetchProjectProperty(flow.getProjectId(),
              flow.getVersion(), node.getId() + ".jor");
    } catch (ProjectManagerException e) {
      e.printStackTrace();
      logger.error("Error loading job override property for job "
          + node.getId());
    }

    File path = new File(execDir, source);
    if (props == null) {
      // if no override prop, load the original one on disk
      try {
        props = new Props(null, path);
      } catch (IOException e) {
        e.printStackTrace();
        logger.error("Error loading job file " + source + " for job "
            + node.getId());
      }
    }
    // setting this fake source as this will be used to determine the location
    // of log files.
    if (path.getPath() != null) {
      props.setSource(path.getPath());
    }

    customizeJobProperties(props);

    return props;
  }

  private void runExecutableNode(ExecutableNode node) throws IOException {
    // Collect output props from the job's dependencies.
    // 准备 node 的环境  -- 将 flow 的环境变量放置到 job 中
    prepareJobProperties(node);

    node.setStatus(Status.QUEUED);
    // 根据job 信息创建一个 jobRunner
    JobRunner runner = createJobRunner(node);
    logger.info("Submitting job '" + node.getNestedId() + "' to run.");

    try {
      executorService.submit(runner);

      logger.info("executorService submit sucess : " + node.getId() + ",status : " + node.getStatus());
      activeJobRunners.add(runner);
      logger.info("active add : " + node.getId());
    } catch (RejectedExecutionException e) {
      logger.error(e);
    }
    ;
  }

  /**
   * Determines what the state of the next node should be. Returns null if the
   * node should not be run.
   *
   * 检查这个node 的依赖等  :
   *      如果这个 node 已经处于运行状态或者，或者依赖节点没有结束，则返回 null （异常）。
   *      如果这个 node 被设置为 disable状态，或者 node 被依赖设置为 skip ，， 则返回状态 skip.
   *      如果这个 node 的依赖被设置为失败或者取消，则这个 node 的状态则设置为取消状态。
   *      如果依赖都结束并且 状态正常， 则设置这个 node 的状态为 ready.
   *
   * @param node
   * @return
   */
  public Status getImpliedStatus(ExecutableNode node) {
    // If it's running or finished with 'SUCCEEDED', than don't even
    // bother starting this job.
    if (Status.isStatusRunning(node.getStatus())
        || node.getStatus() == Status.SUCCEEDED) {
      return null;
    }

    // Go through the node's dependencies. If all of the previous job's
    // statuses is finished and not FAILED or KILLED, than we can safely
    // run this job.
    // 浏览这个节点的依赖的节点，如果所有的依赖是状态完成，而不是 kill 或者 failed
    // 我们可以安全的运行这个节点。
    ExecutableFlowBase flow = node.getParentFlow();
    boolean shouldKill = false;

    // 遍历所有依赖节点的状态，如果没有依赖节点，则忽略这个节点的状态。
    for (String dependency : node.getInNodes()) {
      ExecutableNode dependencyNode = flow.getExecutableNode(dependency);
      Status depStatus = dependencyNode.getStatus();

      if (!Status.isStatusFinished(depStatus)) {
        // 先判断 依赖是否已经结束,如果没有结束则报错
        return null;
      } else if (depStatus == Status.FAILED || depStatus == Status.CANCELLED
          || depStatus == Status.KILLED) {
        // 如果这个node的依赖已经结束，但是并没有正常的终止， 则报错。
        // We propagate failures as KILLED states.
        shouldKill = true;
      }
    }

    // If it's disabled but ready to run, we want to make sure it continues
    // being disabled.
    // 如果这个 node 的信息为 disabled 或者 skipped 状态，则返回 skipped
    if (node.getStatus() == Status.DISABLED
        || node.getStatus() == Status.SKIPPED) {
      return Status.SKIPPED;
    }

    // If the flow has failed, and we want to finish only the currently running
    // jobs, we just
    // kill everything else. We also kill, if the flow has been cancelled.
    // 如果 flow 失败或者 依赖项失败， 则将当前状态设置为 取消状态。
    if (flowFailed
        && failureAction == ExecutionOptions.FailureAction.FINISH_CURRENTLY_RUNNING) {
      return Status.CANCELLED;
    } else if (shouldKill || isKilled()) {
      return Status.CANCELLED;
    }

    // All good to go, ready to run.
    return Status.READY;
  }

  private Props collectOutputProps(ExecutableNode node) {
    Props previousOutput = null;
    // Iterate the in nodes again and create the dependencies
    for (String dependency : node.getInNodes()) {
      Props output =
          node.getParentFlow().getExecutableNode(dependency).getOutputProps();
      if (output != null) {
        output = Props.clone(output);
        output.setParent(previousOutput);
        previousOutput = output;
      }
    }

    return previousOutput;
  }

  private JobRunner createJobRunner(ExecutableNode node) {
    // Load job file.
    File path = new File(execDir, node.getJobSource());

    JobRunner jobRunner =
        new JobRunner(node, path.getParentFile(), executorLoader,
            jobtypeManager);
    if (watcher != null) {
      jobRunner.setPipeline(watcher, pipelineLevel);
    }
    if (validateUserProxy) {
      jobRunner.setValidatedProxyUsers(proxyUsers);
    }

    jobRunner.setFlowOwnUser(flowOwnUser);
    jobRunner.setFlowOwnGroup(flowOwnGroup);
    jobRunner.setFlowLastModifyUser(flowLastModifyUser);

    jobRunner.setDelayStart(node.getDelayedExecution());
    jobRunner.setLogSettings(logger, jobLogFileSize, jobLogNumFiles);
    jobRunner.addListener(listener);

    if (JobCallbackManager.isInitialized()) {
      jobRunner.addListener(JobCallbackManager.getInstance());
    }

    configureJobLevelMetrics(jobRunner);

    return jobRunner;
  }

  /**
   * Configure Azkaban metrics tracking for a new jobRunner instance
   *
   * @param jobRunner
   */
  private void configureJobLevelMetrics(JobRunner jobRunner) {
    logger.info("Configuring Azkaban metrics tracking for jobrunner object");
    if (MetricReportManager.isAvailable()) {
      MetricReportManager metricManager = MetricReportManager.getInstance();

      // Adding NumRunningJobMetric listener
      jobRunner.addListener((NumRunningJobMetric) metricManager
          .getMetricFromName(NumRunningJobMetric.NUM_RUNNING_JOB_METRIC_NAME));

      // Adding NumFailedJobMetric listener
      jobRunner.addListener((NumFailedJobMetric) metricManager
          .getMetricFromName(NumFailedJobMetric.NUM_FAILED_JOB_METRIC_NAME));

    }

    jobRunner.addListener(JmxJobMBeanManager.getInstance());
  }

  public void pause(String user) {
    synchronized (mainSyncObj) {
      if (!flowFinished) {
        logger.info("Flow paused by " + user);
        flowPaused = true;
        flow.setStatus(Status.PAUSED);

        updateFlow();
      } else {
        logger.info("Cannot pause finished flow. Called by user " + user);
      }
    }

    interrupt();
  }

  public void resume(String user) {
    synchronized (mainSyncObj) {
      if (!flowPaused) {
        logger.info("Cannot resume flow that isn't paused");
      } else {
        logger.info("Flow resumed by " + user);
        flowPaused = false;
        if (flowFailed) {
          flow.setStatus(Status.FAILED_FINISHING);
        } else if (flowKilled) {
          flow.setStatus(Status.KILLED);
        } else {
          flow.setStatus(Status.RUNNING);
        }

        updateFlow();
      }
    }

    interrupt();
  }

  public void kill(String user) {
    synchronized (mainSyncObj) {
      logger.info("Flow killed by " + user);
      flow.setStatus(Status.KILLED);
      kill();
      updateFlow();
    }
    interrupt();
  }

  private void kill() {
    synchronized (mainSyncObj) {
      logger.info("Kill has been called on flow " + execId);

      // If the flow is paused, then we'll also unpause
      flowPaused = false;
      flowKilled = true;

      if (watcher != null) {
        logger.info("Watcher is attached. Stopping watcher.");
        watcher.stopWatcher();
        logger
            .info("Watcher cancelled status is " + watcher.isWatchCancelled());
      }

      logger.info("Killing " + activeJobRunners.size() + " jobs.");
      for (JobRunner runner : activeJobRunners) {
        runner.kill();
      }
    }
  }

  public void retryFailures(String user) {
    synchronized (mainSyncObj) {
      logger.info("Retrying failures invoked by " + user);
      retryFailedJobs = true;
      interrupt();
    }
  }

  private void resetFailedState(ExecutableFlowBase flow,
      List<ExecutableNode> nodesToRetry) {
    // bottom up
    LinkedList<ExecutableNode> queue = new LinkedList<ExecutableNode>();
    for (String id : flow.getEndNodes()) {
      ExecutableNode node = flow.getExecutableNode(id);
      queue.add(node);
    }

    long maxStartTime = -1;
    while (!queue.isEmpty()) {
      ExecutableNode node = queue.poll();
      Status oldStatus = node.getStatus();
      maxStartTime = Math.max(node.getStartTime(), maxStartTime);

      long currentTime = System.currentTimeMillis();
      if (node.getStatus() == Status.SUCCEEDED) {
        // This is a candidate parent for restart
        nodesToRetry.add(node);
        continue;
      } else if (node.getStatus() == Status.RUNNING) {
        continue;
      } else if (node.getStatus() == Status.SKIPPED) {
        node.setStatus(Status.DISABLED);
        node.setEndTime(-1);
        node.setStartTime(-1);
        node.setUpdateTime(currentTime);
      } else if (node instanceof ExecutableFlowBase) {
        ExecutableFlowBase base = (ExecutableFlowBase) node;
        switch (base.getStatus()) {
        case CANCELLED:
          node.setStatus(Status.READY);
          node.setEndTime(-1);
          node.setStartTime(-1);
          node.setUpdateTime(currentTime);
          // Break out of the switch. We'll reset the flow just like a normal
          // node
          break;
        case KILLED:
        case FAILED:
        case FAILED_FINISHING:
          resetFailedState(base, nodesToRetry);
          continue;
        default:
          // Continue the while loop. If the job is in a finished state that's
          // not
          // a failure, we don't want to reset the job.
          continue;
        }
      } else if (node.getStatus() == Status.CANCELLED) {
        // Not a flow, but killed
        node.setStatus(Status.READY);
        node.setStartTime(-1);
        node.setEndTime(-1);
        node.setUpdateTime(currentTime);
      } else if (node.getStatus() == Status.FAILED
          || node.getStatus() == Status.KILLED) {
        node.resetForRetry();
        nodesToRetry.add(node);
      }

      if (!(node instanceof ExecutableFlowBase)
          && node.getStatus() != oldStatus) {
        logger.info("Resetting job '" + node.getNestedId() + "' from "
            + oldStatus + " to " + node.getStatus());
      }

      for (String inId : node.getInNodes()) {
        ExecutableNode nodeUp = flow.getExecutableNode(inId);
        queue.add(nodeUp);
      }
    }

    // At this point, the following code will reset the flow
    Status oldFlowState = flow.getStatus();
    if (maxStartTime == -1) {
      // Nothing has run inside the flow, so we assume the flow hasn't even
      // started running yet.
      flow.setStatus(Status.READY);
    } else {
      flow.setStatus(Status.RUNNING);

      // Add any READY start nodes. Usually it means the flow started, but the
      // start node has not.
      for (String id : flow.getStartNodes()) {
        ExecutableNode node = flow.getExecutableNode(id);
        if (node.getStatus() == Status.READY
            || node.getStatus() == Status.DISABLED) {
          nodesToRetry.add(node);
        }
      }
    }
    flow.setUpdateTime(System.currentTimeMillis());
    flow.setEndTime(-1);
    logger.info("Resetting flow '" + flow.getNestedId() + "' from "
        + oldFlowState + " to " + flow.getStatus());
  }

  private void interrupt() {
    flowRunnerThread.interrupt();
  }

  private class JobRunnerEventListener implements EventListener {
    public JobRunnerEventListener() {
    }

    @Override
    public synchronized void handleEvent(Event event) {
      JobRunner runner = (JobRunner) event.getRunner();

      if (event.getType() == Type.JOB_STATUS_CHANGED) {
        updateFlow();
      } else if (event.getType() == Type.JOB_FINISHED) {
        ExecutableNode node = runner.getNode();
        EventData eventData = event.getData();
        long seconds = (node.getEndTime() - node.getStartTime()) / 1000;
        synchronized (mainSyncObj) {
          logger.info("Job " + eventData.getNestedId() + " finished with status "
              + eventData.getStatus() + " in " + seconds + " seconds");

          // Cancellation is handled in the main thread, but if the flow is
          // paused, the main thread is paused too.
          // This unpauses the flow for cancellation.
          if (flowPaused && eventData.getStatus() == Status.FAILED
              && failureAction == FailureAction.CANCEL_ALL) {
            flowPaused = false;
          }

          finishedNodes.add(node);
          node.getParentFlow().setUpdateTime(System.currentTimeMillis());
          interrupt();
          fireEventListeners(event);
        }
      }
    }
  }

  public boolean isKilled() {
    return flowKilled;
  }

  public ExecutableFlow getExecutableFlow() {
    return flow;
  }

  public File getFlowLogFile() {
    return logFile;
  }

  public File getJobLogFile(String jobId, int attempt) {
    ExecutableNode node = flow.getExecutableNodePath(jobId);
    File path = new File(execDir, node.getJobSource());

    String logFileName = JobRunner.createLogFileName(node, attempt);
    File logFile = new File(path.getParentFile(), logFileName);

    if (!logFile.exists()) {
      return null;
    }

    return logFile;
  }

  public File getJobAttachmentFile(String jobId, int attempt) {
    ExecutableNode node = flow.getExecutableNodePath(jobId);
    File path = new File(execDir, node.getJobSource());

    String attachmentFileName =
        JobRunner.createAttachmentFileName(node, attempt);
    File attachmentFile = new File(path.getParentFile(), attachmentFileName);
    if (!attachmentFile.exists()) {
      return null;
    }
    return attachmentFile;
  }

  public File getJobMetaDataFile(String jobId, int attempt) {
    ExecutableNode node = flow.getExecutableNodePath(jobId);
    File path = new File(execDir, node.getJobSource());

    String metaDataFileName = JobRunner.createMetaDataFileName(node, attempt);
    File metaDataFile = new File(path.getParentFile(), metaDataFileName);

    if (!metaDataFile.exists()) {
      return null;
    }

    return metaDataFile;
  }

  public boolean isRunnerThreadAlive() {
    if (flowRunnerThread != null) {
      return flowRunnerThread.isAlive();
    }
    return false;
  }

  public boolean isThreadPoolShutdown() {
    return executorService.isShutdown();
  }

  public int getNumRunningJobs() {
    return activeJobRunners.size();
  }

  public int getExecutionId() {
    return execId;
  }
}
