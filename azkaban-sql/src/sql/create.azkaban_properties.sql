CREATE TABLE `azkaban_flow_properties` (
  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `property_user` varchar(50) NOT NULL COMMENT 'env properties flag',
  `property_key` varchar(50) NOT NULL COMMENT 'process env key',
  `property_value` varchar(512) NOT NULL COMMENT 'process env value',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8;