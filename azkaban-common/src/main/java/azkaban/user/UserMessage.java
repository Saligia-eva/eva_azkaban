package azkaban.user;

import org.joda.time.DateTime;
import org.joda.time.Period;

import javax.xml.crypto.Data;
import java.util.Set;

/**
 * Created by saligia on 17-8-16.
 */
public class UserMessage {
    private int userId = 0;
    private String userName = null;
    private boolean userIsDisable = false;
    private String userDisableTime = null;
    private String userPeriod = null;

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }


    public boolean getUserIsDisable() {
        return userIsDisable;
    }

    public void setUserIsDisable(boolean userIsDisable) {
        this.userIsDisable = userIsDisable;
    }


    public String getUserDisableTime() {
        return userDisableTime;
    }

    public void setUserDisableTime(String userDisableTime) {
        this.userDisableTime = userDisableTime;
    }

    public String getUserPeriod() {
        return userPeriod;
    }

    public void setUserPeriod(String userPeriod) {
        this.userPeriod = userPeriod;
    }



    @Override
    public String toString() {
        return "UserMessage{" +
                "userName='" + userName + '\'' +
                ", userIsDisable=" + userIsDisable +
                '}';
    }
}
