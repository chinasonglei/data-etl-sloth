#!/bin/bash
source /etc/azkaban/bigdata_azkaban_passwd_we.sh
#
# cd source/paltform/az_dispatch && mvn packages
# cd source/paltform/task_monitoring && mvn packages
# cd source/paltform/meta_manage && mvn packages
# sh -x source/platform/sbin/autoscp.sh

zip -r flow.zip flow
/opt/anaconda3/bin/python script/deploy/deploy.py
rm -rf flow.zip



# for i in {'bdw_beadwalletloan_bw_repayment_plan','bdw_beadwalletloan_bw_overdue_record','bdw_beadwalletloan_bw_order','bdw_beadwalletloan_bw_order_channel','bdw_beadwalletloan_bw_order_status','bdw_beadwalletloan_bw_payment_record','bdw_beadwalletloan_bw_borrower','bdw_beadwalletloan_bw_check_record','bdw_beadwalletloan_bw_reject_record','bdw_beadwalletloan_bw_rule_order_log','bdw_beadwalletloan_bw_cloud_external','sqq_sassevenwallet_bw_repayment_plan','sqq_sassevenwallet_bw_overdue_record','sqq_sassevenwallet_bw_order','sqq_sassevenwallet_bw_order_channel','sqq_sassevenwallet_bw_order_status','sqq_sassevenwallet_bw_payment_record','sqq_sassevenwallet_bw_borrower','sqq_sassevenwallet_bw_check_record','sqq_sassevenwallet_bw_reject_record','sqq_sassevenwallet_bw_rule_order_log','sqq_sassevenwallet_bw_cloud_external','udw_approval_cac_eval_main'}
# do
# cp /home/dispatch/platform/etl_dispatch/schedule/etl_script/schedule/app_check/app_check_job/check_app_${i}.job ./job/
# done
