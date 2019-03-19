USE sparkdb;

DROP TABLE IF EXISTS `word_count_result`;

CREATE TABLE IF NOT EXISTS `word_count_result` (
  `count_time` DATETIME    NOT NULL,
  `word`       VARCHAR(30) NOT NULL,
  `count`      INT(10)     NOT NULL,
  PRIMARY KEY (`count_time`, `word`)
)
  ENGINE = INNODB
  DEFAULT CHARSET = utf8;

SELECT *
FROM word_count_result
ORDER BY count_time, word ASC;