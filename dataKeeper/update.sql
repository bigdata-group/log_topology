
create table if not exists goods_pv (
id int(32) NOT NULL,
pv int(11),
uv int(11),
store_pv int(11),
store_uv int(11),
UNIQUE KEY `UK_goods_id` (`id`)
)engine=innodb DEFAULT CHARSET=utf8mb4;


create table if not exists goods_pv_total_tmp (
id int(32) NOT NULL,
pv int(11),
uv int(11),
store_pv int(11),
store_uv int(11),
UNIQUE KEY `UK_goods_id` (`id`)
)engine=innodb DEFAULT CHARSET=utf8mb4;


create table if not exists goods_pv_total (
id int(32) NOT NULL,
pv int(11),
uv int(11),
store_pv int(11),
store_uv int(11),
UNIQUE KEY `UK_goods_id` (`id`)
)engine=innodb DEFAULT CHARSET=utf8mb4;


DELETE FROM goods_pv_total WHERE 1;


INSERT INTO goods_pv_total (id, pv, uv, store_pv, store_uv)
select 
id,
sum(pv),
sum(uv),
sum(store_pv),
sum(store_uv)
from
(
select *
from goods_pv_total_tmp
union all 
select *
from goods_pv
) b
group by id;


DELETE FROM goods_pv_total_tmp WHERE 1;


INSERT INTO goods_pv_total_tmp (id, pv, uv, store_pv, store_uv)
select * from goods_pv_total;


create table if not exists features_pv (
id int(32) NOT NULL,
pv int(11),
uv int(11),
store_pv int(11),
store_uv int(11),
UNIQUE KEY `UK_features_id` (`id`)
)engine=innodb DEFAULT CHARSET=utf8mb4;


create table if not exists features_pv_total_tmp (
id int(32) NOT NULL,
pv int(11),
uv int(11),
store_pv int(11),
store_uv int(11),
UNIQUE KEY `UK_features_id` (`id`)
)engine=innodb DEFAULT CHARSET=utf8mb4;


create table if not exists features_pv_total (
id int(32) NOT NULL,
pv int(11),
uv int(11),
store_pv int(11),
store_uv int(11),
UNIQUE KEY `UK_features_id` (`id`)
)engine=innodb DEFAULT CHARSET=utf8mb4;


DELETE FROM features_pv_total WHERE 1;


INSERT INTO features_pv_total (id, pv, uv, store_pv, store_uv)
select 
id,
sum(pv),
sum(uv),
sum(store_pv),
sum(store_uv)
from
(
select *
from features_pv_total_tmp
union all 
select *
from features_pv
) b
group by id;


DELETE FROM features_pv_total_tmp WHERE 1;


INSERT INTO features_pv_total_tmp (id, pv, uv, store_pv, store_uv)
select * from features_pv_total;


