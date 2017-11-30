 CREATE TABLE `azkaban_user_manager` (
  `user_id` int(11) NOT NULL AUTO_INCREMENT,
  `user_name` varchar(50) NOT NULL,
  `user_password` varchar(100) NOT NULL,
  `user_mail` varchar(100) DEFAULT NULL,
  `user_is_disable` tinyint(2) NOT NULL,
  `user_disable_time` datetime DEFAULT NULL,
  `user_disable_period` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`user_id`),
  UNIQUE KEY `user_name` (`user_name`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8 COMMENT='utf8_general_ci';

CREATE TABLE `azkaban_group_manager` (
  `group_id` int(11) NOT NULL AUTO_INCREMENT,
  `group_name` varchar(50) NOT NULL,
  `group_role` varchar(50) NOT NULL,
  `group_proxy` varchar(512) NOT NULL,
  PRIMARY KEY (`group_id`),
  UNIQUE KEY `group_name` (`group_name`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 COMMENT='utf8_general_ci';

CREATE TABLE `azkaban_roles_manager` (
  `role_name` varchar(50) NOT NULL,
  `role_permission` int(12) NOT NULL,
  UNIQUE KEY `role_name` (`role_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='utf8_general_ci';

CREATE TABLE `azkaban_user_group` (
  `user_id` int(11) NOT NULL,
  `group_id` int(11) NOT NULL,
  UNIQUE KEY `user_id` (`user_id`,`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='utf8_general_ci';