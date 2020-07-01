drop table if exists ods.ods_bdw_beadwalletloan_activity_discount_info ;
create table ods.ods_bdw_beadwalletloan_activity_discount_info (
`discount_id` bigint comment '主键',
`activity_id` bigint comment '活动基本信息表主键',
`bonus_amount` string comment '奖励金额',
`number` bigint comment '优惠券数量',
`loan_amount` string comment '限定金额',
`invited_number` bigint comment '受邀请人数',
`create_time` string comment '创建时间',
`instructions` string comment '使用说明',
`yhq_start_time` string comment '优惠券生效时间',
`yhq_end_time` string comment '优惠券到期时间',
`yhq_status` int comment '是否启用:1:启用，0：不启用',
`yhq_remark` string comment '优惠券备注',
`operator` string comment '优惠券操作人',
`probability` string comment '中奖概率',
`img` string comment '实物图片',
`type` bigint comment '奖品类型 1优惠券；2实物；3未中奖；4免息券',
`prize_name` string comment '奖品名称',
`prize_total` bigint comment '奖品总数',
`prize_surplus` bigint comment '奖品剩余数量',
`update_time` string comment '',
`is_open` bigint comment '是否开启每天至少中一次免息功能 0不开启，1开启',
`sort` bigint comment '排序' )comment '活动优惠信息表'partitioned by (dt int) stored as parquet location 'hdfs:///user/hive/warehouse/ods/ods_bdw_beadwalletloan_activity_discount_info' ;