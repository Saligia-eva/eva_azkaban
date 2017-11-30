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

$.namespace('azkaban');

var loginView;
azkaban.LoginView = Backbone.View.extend({
  events: {
    "click #login-submit": "handleLogin",
    'keypress input': 'handleKeyPress'
  },

  initialize: function(settings) {
    $('#error-msg').hide();
  },

  handleLogin: function(evt) {
    console.log("Logging in.");
    saveCookie();
    var username = $("#username").val();
    var password = $("#password").val();

    $.ajax({
      async: "false",
      url: contextURL,
      dataType: "json",
      type: "POST",
      data: {
        action: "login",
        username: username,
        password: password
      },
      success: function(data) {
        if (data.error) {
          $('#error-msg').text(data.error);
          $('#error-msg').slideDown('fast');
        }
        else {
          document.location.reload();
        }
      }
    });
  },

  handleKeyPress: function(evt) {
    if (evt.charCode == 13 || evt.keyCode == 13) {
      this.handleLogin();
    }
  },

  render: function() {
  }
});

function getCookie(){
    if ($.cookie("rmbUser") == "true") {
        $("#ck_rmbUser").attr("checked", true);
        $("#username").val($.cookie("username"));
        $("#password").val($.cookie("password"));
     }
}


function saveCookie(){
        if ($("#ck_rmbUser").is(":checked")) {
          var str_username = $("#username").val();
          var str_password = $("#password").val();
          $.cookie("rmbUser", "true", { expires: 7 });        //存储一个带7天期限的cookie
          $.cookie("username", str_username, { expires: 7 });
          $.cookie("password", str_password, { expires: 7 });
        }
        else {
          $.cookie("rmbUser", "false", { expire: -1 });
          $.cookie("username", "", { expires: -1 });
          $.cookie("password", "", { expires: -1 });
        }
}

$(function() {
  getCookie();
  loginView = new azkaban.LoginView({el: $('#login-form')});
});


