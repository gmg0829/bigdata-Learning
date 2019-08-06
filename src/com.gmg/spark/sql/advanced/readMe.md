CREATE TABLE `access` (
  `remote_addr` varchar(255) DEFAULT NULL,
  `remote_user` varchar(255) DEFAULT NULL,
  `time_local` varchar(255) DEFAULT NULL,
  `request` varchar(255) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `byte_sent` varchar(255) DEFAULT NULL,
  `refere` varchar(255) DEFAULT NULL,
  `http_agent` varchar(255) DEFAULT NULL,
  `http_forward_for` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `acc_addr_count` (
  `remote_addr` varchar(255) DEFAULT NULL,
  `count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;