CREATE TABLE `executor_user` (
    `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'primary key',
    `exec_id` int(11) NOT NULL COMMENT 'executor id in table executors',
    `username` varchar(64) NOT NULL COMMENT 'configed in user.xml',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uniq_exec_user` (username, exec_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;