USE sparkdb;

DROP TABLE IF EXISTS `day_video_topn_stat`;

CREATE TABLE IF NOT EXISTS `day_video_topn_stat`(
   `day` VARCHAR(8) NOT NULL,
   `cmsid` BIGINT(10) NOT NULL,
   `times` BIGINT(10) NOT NULL,
   PRIMARY KEY ( `day`,`cmsid` )
)ENGINE=INNODB DEFAULT CHARSET=utf8;

SELECT * FROM day_video_topn_stat ORDER BY times DESC;


DROP TABLE IF EXISTS `day_video_city_topn_stat`;


CREATE TABLE IF NOT EXISTS `day_video_city_topn_stat` (
  `day` varchar(8) NOT NULL,
  `cmsid` bigint(10) NOT NULL,
  `city` varchar(20) NOT NULL,
  `times` bigint(8) NOT NULL,
  `times_rank` int(11) NOT NULL,
  PRIMARY KEY (`day`,`cmsid`,`city`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

SELECT * FROM day_video_city_topn_stat ORDER BY city ASC, times_rank ASC;
