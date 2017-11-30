package azkaban.user;

import java.util.HashSet;

/**
 * Created by saligia on 17-8-24.
 */
public class MysqlAzkabanUser {
    private String userId = null;
    private String userName = null;
    private String userPasswd = null;
    private String userMail = null;
    private boolean userDisable = false;
    private String disableTime = null;
    private String disablePeriod = null;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserPasswd() {
        return userPasswd;
    }

    public void setUserPasswd(String userPasswd) {
        this.userPasswd = userPasswd;
    }

    public String getUserMail() {
        return userMail;
    }

    public void setUserMail(String userMail) {
        this.userMail = userMail;
    }

    public boolean isUserDisable() {
        return userDisable;
    }

    public void setUserDisable(boolean userDisable) {
        this.userDisable = userDisable;
    }

    public String getDisableTime() {
        return disableTime;
    }

    public void setDisableTime(String disableTime) {
        this.disableTime = disableTime;
    }

    public String getDisablePeriod() {
        return disablePeriod;
    }

    public void setDisablePeriod(String disablePeriod) {
        this.disablePeriod = disablePeriod;
    }

    @Override
    public String toString() {
        return "AzkabanUser{" +
                "userId='" + userId + '\'' +
                ", userName='" + userName + '\'' +
                ", userPasswd='" + userPasswd + '\'' +
                ", userMail='" + userMail + '\'' +
                ", userDisable=" + userDisable +
                ", disableTime='" + disableTime + '\'' +
                ", disablePeriod='" + disablePeriod + '\'' +
                '}';
    }
}
