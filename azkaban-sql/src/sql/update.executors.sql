ALTER TABLE `executors` ADD COLUMN `type` tinyint(1) DEFAULT '0' COMMENT '0: 正常executor, 1: backup executor';