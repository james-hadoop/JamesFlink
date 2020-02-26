# JamesFlink

### socket stream data source
```
nc -lk 7777
```

### Redis
```
sudo docker run -d --name james-redis-server -p 6379:6379 -v /home/james/data/redis/data:/data -v /home/james/data/redis/conf/redis.conf:/usr/local/etc/redis/redis.conf redis:latest redis-server /usr/local/etc/redis/redis.conf --appendonly yes
```

### Mysql tables
```
CREATE TABLE `src_qiyong_ex_cnt_output` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `src` int(11) NOT NULL COMMENT '来源ID',
  `current_cnt` mediumtext NOT NULL COMMENT '当前内容量',
  `low_cnt` mediumtext NOT NULL COMMENT '异常低位阈值，低于该值位异常',
  `high_cnt` mediumtext NOT NULL COMMENT '异常高位阈值，高于该值位异常',
  `event_time` datetime NOT NULL COMMENT '事件时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  KEY `idx_id` (`id`),
  KEY `idx_src_id` (`src`),
  KEY `idx_event_time` (`event_time`)
) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARSET=utf8;

```
```
CREATE TABLE `src_qiyong_ex_cnt_threshold` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `src` int(11) NOT NULL COMMENT '来源ID',
  `low_cnt` mediumtext NOT NULL COMMENT '异常低位阈值，低于该值位异常',
  `high_cnt` mediumtext NOT NULL COMMENT '异常高位阈值，高于该值位异常',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  KEY `inx_id` (`id`),
  KEY `inx_src_id` (`src`),
  KEY `inx_update_time` (`update_time`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8;

```