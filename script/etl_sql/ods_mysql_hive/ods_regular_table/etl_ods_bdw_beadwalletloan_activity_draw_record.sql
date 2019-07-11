select 
`id`,
`borrower_id`,
`activity_id`,
`is_winning`,
`prize_id`,
`grant_status`,
`contacts_name`,
`contacts_phone`,
`address`,
DATE_FORMAT(create_time, "%Y-%m-%d %H:%i:%s") AS create_time,
DATE_FORMAT(update_time, "%Y-%m-%d %H:%i:%s") AS update_time
 from activity_draw_record a