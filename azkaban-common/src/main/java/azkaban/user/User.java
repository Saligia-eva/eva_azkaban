/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.user;

import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.*;

public class User {
  private final String userid;
  private String email = "";
  private Set<String> roles = new HashSet<String>();
  private Set<String> groups = new HashSet<String>();
  private Map<String,String> groupRolesmap = new HashMap<String, String>();
  // 用户禁用标志
  private boolean isDisable = false;

  private UserPermissions userPermissions;

  private HashMap<String, String> properties = new HashMap<String, String>();

  public User(String userid) {
    this.userid = userid;
  }

  public User(MysqlAzkabanUser user){
    this.userid = user.getUserName();
    this.email = user.getUserMail();
    this.isDisable = user.isUserDisable();
  }

  public String getUserId() {
    return userid;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public String getEmail() {
    return email;
  }

  public boolean getIsDisable(){
    return isDisable;
  }

  public void setIsDisable(boolean isDisable){
    this.isDisable = isDisable;
  }

  public void setPermissions(UserPermissions checker) {
    this.userPermissions = checker;
  }

  public UserPermissions getPermissions() {
    return userPermissions;
  }

  public boolean hasPermission(String permission) {
    if (userPermissions == null) {
      return false;
    }
    return this.userPermissions.hasPermission(permission);
  }

  public Map<String, String> getGroupRolesmap(){
    return this.groupRolesmap;
  }

  public List<String> getGroups() {
    return new ArrayList<String>(groups);
  }

  public void clearGroup() {
    groups.clear();
  }

  public void addGroup(String name) {
    groups.add(name);
  }

  public boolean isInGroup(String group) {
    return this.groups.contains(group);
  }

  public List<String> getRoles() {
    return new ArrayList<String>(roles);
  }

  public void addRole(String role) {
    this.roles.add(role);
  }

  public void clearRole(){
    roles.clear();
  }
  public boolean hasRole(String role) {
    return roles.contains(role);
  }

  public String getProperty(String name) {
    return properties.get(name);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((userid == null) ? 0 : userid.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    User other = (User) obj;
    if (userid == null) {
      if (other.userid != null)
        return false;
    } else if (!userid.equals(other.userid))
      return false;
    return true;
  }


  @Override
  public String toString() {

    String roleStr = "";

    for(String role : roles){
      roleStr += role + ";";
    }

    String groupStr = "";

    for(String group : groups){
      groupStr += group +";";
    }

    return "User{" +
            "userid='" + userid + '\'' +
            ", email='" + email + '\'' +
            ", roles=" + roleStr +
            ", groups=" + groupStr +
            ", isDisable=" + isDisable +
            '}';
  }

  public static interface UserPermissions {
    public boolean hasPermission(String permission);

    public void addPermission(String permission);
  }

  public static class DefaultUserPermission implements UserPermissions {
    Set<String> permissions;

    public DefaultUserPermission() {
      this(new HashSet<String>());
    }

    public DefaultUserPermission(Set<String> permissions) {
      this.permissions = permissions;
    }

    @Override
    public boolean hasPermission(String permission) {
      return permissions.contains(permission);
    }

    @Override
    public void addPermission(String permission) {
      permissions.add(permission);
    }
  }
}
