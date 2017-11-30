package azkaban.user;

import azkaban.database.AbstractJdbcLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.utils.Props;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by saligia on 17-8-16.
 */
public class MysqlUserManager extends AbstractJdbcLoader implements UserManager {

    private static final Logger logger = Logger.getLogger(MysqlUserManager.class
            .getName());


    public MysqlUserManager(Props props) {
        super(props);
    }
    @Override
    public User getUser(String username, String password) throws UserManagerException {

        QueryRunner queryRunner = createQueryRunner();
        LinkedList<MysqlAzkabanUser> users = null;

        try{
            users = queryRunner.query(FetchAzkabanUesr.AZKABANUSERBYADMIN, new FetchAzkabanUesr(), username, password);
        }catch (SQLException e) {
            throw  new UserManagerException("[SQl query error : " + FetchAzkabanUesr.AZKABANUSERBYADMIN + "]" + e);
        }


        if(users.size() == 0){
            throw new UserManagerException("Username or password error");
        }

        MysqlAzkabanUser user = users.get(0);

        logger.info("get user : " + user);

        if(user.isUserDisable()){
            try{
                String userPeriod = user.getDisablePeriod();
                String userDisableTime = user.getDisableTime();

                if(userPeriod == null || userDisableTime == null){
                    throw new IllegalArgumentException();
                }

                DateTime stopTime = DateTime.parse(userDisableTime, DateTimeFormat.forPattern("yyyy-MM-dd")).plus(Period.parse(userPeriod));

                if(stopTime.getMillis() > DateTime.now().getMillis()){  // 处于禁用状态
                    throw new UserManagerException("您由于非法操作，账号被禁用 (到期日期 :" + stopTime.toString() + "). 有其他问题请联系调度平台管理员");
                }else{                                                  // 禁用到期
                    user.setUserDisable(false);
                    queryRunner.update(FetchAzkabanUesr.UPDATEUSERFLAG,user.getDisablePeriod(), user.getUserId());
                }
            }catch (IllegalArgumentException e){
                logger.error("user login error : " + e.getMessage());
                user.setUserDisable(false);
                try {
                    queryRunner.update(FetchAzkabanUesr.UPDATEUSERFLAG, "P7D", user.getUserId());
                } catch (SQLException e1) {
                    throw new UserManagerException("UPDATE ERROR : " + e);
                }
            } catch (SQLException e) {
                throw new UserManagerException("UPDATE ERROR : " + e);
            }
        }

        List<MysqlAzkabanGroup> groups = null;

        try{
            groups = queryRunner.query(FetchAzkabanGroup.AZKABANGROUPBYUSER, new FetchAzkabanGroup(), user.getUserId());
        } catch (SQLException e){
            throw new UserManagerException("sql error");
        }

        User loginUser = new User(user);

        for(MysqlAzkabanGroup group : groups){
            loginUser.addGroup(group.getGroupName());
            loginUser.addRole(group.getGroupRole());
            loginUser.getGroupRolesmap().put(group.getGroupName(), group.getGroupRole());
        }

        if(loginUser.getGroups().size() == 0){
            throw new UserManagerException("user group is invalidity");
        }
        // 获取用户组信息
        logger.info("login : " + loginUser);
        return loginUser;

    }
    @Override
    public boolean validateUser(String username) {
        QueryRunner runner = createQueryRunner();

        List<MysqlAzkabanUser> users = null;

        try {
            users =  runner.query(FetchAzkabanUesr.AZKABANUSERBYNAME, new FetchAzkabanUesr(), username);
        } catch (SQLException e) {
            logger.error("Get validate user error : " + e);
            return false;
        }

        if(users.size() == 0){
            return false;
        }else{
            return true;
        }
    }

    @Override
    public boolean validateGroup(String group) {
        QueryRunner runner = createQueryRunner();

        List<MysqlAzkabanGroup> groups = null;

        try {
            groups =  runner.query(FetchAzkabanGroup.AZKABANGROUPBYNAME, new FetchAzkabanGroup(), group);
        } catch (SQLException e) {
            logger.error("Get validate group error : " + e);
            return false;
        }

        if(groups.size() == 0){
            return false;
        }else{
            return true;
        }
    }

    /*
    * 从数据库中获取角色权限
     */
    @Override
    public Role getRole(String roleName) {

        QueryRunner runner = createQueryRunner();
        LinkedList<Role>  roles = null;

        try{
            roles = runner.query(FetchRoleHandel.GETROLEBYNAME, new FetchRoleHandel(), roleName);
        } catch (SQLException e) {
            logger.error("[SQL query error : " + FetchRoleHandel.GETROLEBYNAME +  "]" + e);
            return null;
        }

        if(roles.size() == 0){
            return null;
        }else {
            logger.info("get role : " + roles.get(0).getName() +  "[" + roles.get(0).getPermission() +"]");
            return roles.get(0);
        }
    }
    /*
    * 刷新用户信息
     */
    @Override
    public boolean validateProxyUser(String proxyUser, User realUser) {

        QueryRunner runner = createQueryRunner();
        Set<String> proxys = new HashSet<String>();

        logger.info("proxyUser test : " + proxyUser + "; " + realUser );

        try {
            for(String groupName : realUser.getGroups()){
               LinkedList<MysqlAzkabanGroup> groups = runner.query(FetchAzkabanGroup.AZKABANGROUPBYNAME, new FetchAzkabanGroup(), groupName);

               for(MysqlAzkabanGroup group : groups){
                   proxys.addAll(group.getGroupProxySet());
               }
            }

        } catch (SQLException e) {
           logger.error("[SQL query erro : " + FetchAzkabanGroup.AZKABANGROUPBYNAME + "]" + e);
           return false;
        }

        if(proxys.contains(proxyUser)){
            return true;
        }

        return false;
    }
    /*
     * 获取用户规则
     */
    private static class FetchRoleHandel implements ResultSetHandler<LinkedList<Role>>{

        private static String GETROLEBYNAME = "SELECT role_name, role_permission FROM azkaban_roles_manager where role_name=?;";
        @Override
        public LinkedList<Role> handle(ResultSet rs) throws SQLException {

            LinkedList<Role> roles = new LinkedList<Role>();

            while(rs.next()){
                String name = rs.getString("role_name");
                Permission permission = new Permission(rs.getInt("role_permission"));

                Role role = new Role(name, permission);
                roles.add(role);
            }
            return roles;
        }
    }
    /*
    * 用户组信息
     */
    private static class FetchAzkabanGroup implements
            ResultSetHandler<LinkedList<MysqlAzkabanGroup>>{

        private static String AZKABANGROUPBYNAME = "SELECT group_id,group_name, group_role,group_proxy FROM azkaban_group_manager WHERE group_name=?";
        private static String AZKABANGROUP = "SELECT group_id,group_name, group_role,group_proxy FROM azkaban_group_manager";
        private static String AZKABANGROUPBYUSER = "SELECT groups.group_id, groups.group_name, groups.group_role, groups.group_proxy FROM azkaban_user_group usergroup JOIN azkaban_group_manager groups ON usergroup.group_id=groups.group_id WHERE usergroup.user_id=?;";

        @Override
        public LinkedList<MysqlAzkabanGroup> handle(ResultSet rs) throws SQLException {

            LinkedList<MysqlAzkabanGroup> groups = new LinkedList<MysqlAzkabanGroup>();

            while(rs.next()){
                MysqlAzkabanGroup group = new MysqlAzkabanGroup();
                group.setGroupId(rs.getString("group_id"));
                group.setGroupName(rs.getString("group_name"));
                group.setGroupRole(rs.getString("group_role"));
                group.setGroupProxy(rs.getString("group_proxy"));
                groups.add(group);
            }
            return groups;
        }
    }
    /*
    * 获取指定用户对应的信息表
     */
    private static class FetchAzkabanUesr implements
            ResultSetHandler<LinkedList<MysqlAzkabanUser>>{

        private static String UPDATEUSERFLAG =
                "UPDATE azkaban_user_manager SET user_is_disable=0, user_disable_time=null, user_disable_period=? where user_id=?;";
        private static String AZKABANUSERBYADMIN = "SELECT user_id,user_name, user_password,user_mail,user_is_disable,date_format(user_disable_time,'%Y-%m-%d') user_disable_time,user_disable_period FROM azkaban_user_manager WHERE user_name =? AND user_password=?";
        private static String AZKABANUSERBYNAME = "SELECT user_id,user_name, user_password,user_mail,user_is_disable,date_format(user_disable_time,'%Y-%m-%d') user_disable_time,user_disable_period FROM azkaban_user_manager WHERE user_name =?";
        private static String AZKABANALLUSER = "SELECT user_id,user_name, user_password,user_mail,user_is_disable,date_format(user_disable_time,'%Y-%m-%d') user_disable_time,user_disable_period FROM azkaban_user_manager";
        @Override
        public LinkedList<MysqlAzkabanUser> handle(ResultSet rs) throws SQLException {

            LinkedList<MysqlAzkabanUser> users = new LinkedList<MysqlAzkabanUser>();

            while(rs.next()){
                MysqlAzkabanUser user = new MysqlAzkabanUser();
                user.setUserId(rs.getString("user_id"));
                user.setUserName(rs.getString("user_name"));
                user.setUserPasswd(rs.getString("user_password"));
                user.setUserMail(rs.getString("user_mail"));
                user.setUserDisable(rs.getBoolean("user_is_disable"));
                user.setDisableTime(rs.getString("user_disable_time"));
                user.setDisablePeriod(rs.getString("user_disable_period"));
                users.add(user);
            }

            return users;
        }
    }
}
