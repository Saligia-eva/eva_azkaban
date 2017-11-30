package azkaban.user;

import java.util.HashSet;

/**
 * Created by saligia on 17-8-24.
 */
public class MysqlAzkabanGroup {
    private String groupId = null;
    private String groupName = null;
    private String groupRole = null;
    private HashSet<String> groupProxys = new HashSet<String>();

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getGroupRole() {
        return groupRole;
    }

    public void setGroupRole(String groupRole) {
        this.groupRole = groupRole;
    }

    public String getGroupProxys() {
        String proxys = "";

        for(String proxy : groupProxys){
            proxys += proxy + ";";
        }

        if(proxys.length() != 0){
            proxys = proxys.substring(0, proxys.length()-1);
        }
        return proxys;
    }

    public HashSet<String> getGroupProxySet(){
        return groupProxys;
    }

    public void setGroupProxy(String groupProxy) {
        groupProxys.clear();
        if(groupProxy != null && groupProxy.length() != 0){
            for(String proxy : groupProxy.split(";")){
                groupProxys.add(proxy);
            }
        }
    }


    public void addGroupProxy(String groupProxy){
        if(groupProxy != null){
            groupProxys.add(groupProxy);
        }
    }



    @Override
    public String toString() {
        return "AzkabanGroup{" +
                "groupId='" + groupId + '\'' +
                ", groupName='" + groupName + '\'' +
                ", groupRole='" + groupRole + '\'' +
                '}';
    }
}
