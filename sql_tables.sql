CREATE TABLE `gpu_nodes` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `hypervisor` varchar(64) NOT NULL,
  `gpu_type` char(16) NOT NULL,
  `pci_id` varchar(32) NOT NULL,
  `active` tinyint(1) DEFAULT NULL,
  `tmp_active` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `hypervisor_pci_id` (`hypervisor`,`pci_id`),
  KEY `hypervisor` (`hypervisor`),
  KEY `gpu_type` (`gpu_type`)
) ENGINE=InnoDB AUTO_INCREMENT=17635 DEFAULT CHARSET=latin1 ;

CREATE TABLE `ip2project` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `ip` varchar(64) DEFAULT NULL,
  `project_name` varchar(255) DEFAULT NULL,
  `project_uuid` varchar(255) DEFAULT NULL,
  `start_date` date DEFAULT NULL,
  `end_date` date DEFAULT NULL,
  `email` varchar(128) DEFAULT NULL,
  `instance_uuid` varchar(255) DEFAULT NULL,
  `instance_name` varchar(255) DEFAULT NULL,
  `instance_launched_at` datetime DEFAULT NULL,
  `instance_terminated_at` datetime DEFAULT NULL,
  `final` int(1) DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ip_uuid_triple` (`ip`,`project_uuid`,`instance_uuid`),
  KEY `ip` (`ip`),
  KEY `email` (`email`)
) ENGINE=InnoDB AUTO_INCREMENT=41328 DEFAULT CHARSET=latin1 ;

CREATE TABLE `ip2project_gpu_nodes` (
  `ip2project_id` int(11) NOT NULL,
  `gpu_node_id` int(11) NOT NULL,
  PRIMARY KEY (`ip2project_id`,`gpu_node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

