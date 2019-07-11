drop table if exists rds.rds_bdw_beadwalletloan_activity_draw_record ;
create table rds.rds_bdw_beadwalletloan_activity_draw_record (`id` bigint comment '',
`borrower_id` bigint comment '用户id',
`activity_id` bigint comment '活动id',
`is_winning` bigint comment '是否中奖 0未中奖；1中奖',
`prize_id` bigint comment '奖品id',
`grant_status` bigint comment '发放状态 0未发放；1已发放；2确认收货',
`contacts_name` string comment '联系人姓名',
`contacts_phone` string comment '联系人手机',
`address` string comment '收货地址',
`create_time` string comment '中奖时间',
`update_time` string comment '' )comment '抽奖记录表'partitioned by (is_valid int,ce_start_date int,ce_end_date int) stored as parquet;