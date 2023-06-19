#!/bin/bash

# 设置目录
# 执行命令时，需要更改inpu_dir的路径为文件所在目录
input_dir="/share/data/CCRM/CDFile/fromCODMS/temp"

# 设置log目录
lOG_FILE=/home/ccrm/batch/control-m/logs/load_inchag_data_work_tsk_rnmd.log

# 设置数据日期
DT=$(mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -N -e "SELECT PARAM_VAL FROM ccrm.sys_param_config where PARAM_ID='dataBatchDate'")
DATA_DT=$(date -d"$DT +1 days" +"%Y%m%d")
DT_DATA=$(date -d"$DT +1 days" +"%Y-%m-%d")


# 数据库
DB_NAME="CCRM"


# 定义数据库连接函数
function mysql_exec(){
	mysql -u$DB_USER -p$DB_PASS -h$DB_HOST $DB_NAME
}


# 删除原log
rm -rf $LOG_FILE


#  定义存储相关联的表
tables=$(mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -N -e "SELECT DEP_FLG from CCRM.DATA_BATCH_CONFIG where PROC_NAME='PROC_WORK_TSK_RMND';")


# 计算关联的表的个数
DEP_COUNT=$(echo $tables | wc -w)

# 循环检测数据库配置表的LOAD_STATUS状态
while [ -n "$(echo -e "${tables}" | tr -d '[:space:]')" ]j;do
	for table in $tables;do
		LOAD_STATUS=$(mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -N -e "SELECT STATUS FROM CCRM.DATA_BATCH_CONFIG WHERE PROC_NAME='$table'")
		# 统计LOAD_STATUS=0的数量
		if [ $LOAD_STATUS == 2 ];then
			DEP_COUNT=$[$DEP_COUNT-1]
			if [ $DEP_COUNT == 0 ];then
				# 修改参数为执行成功
				mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -e "update data_batch_config set data_arrive_time_or_proc_name=now() where proc_name='PROC_WORK_TSK_RNMD';"
				mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -e "update data_batch_config set status='2' where proc_name='$table';"
				mysql --defualts-extra-file=/home/ccrm/secret.cnf ccrm -N -e "call proc_work_tsk_rmnd('$DT';)" >> $LOG_FILE
				if [ $? = 0 ];then
					echo "RC=0,成功，message=work_tsk_rnmd 存储过程执行成功！" | tee -a $LOG_FILE
					# 修改参数为默认状态
					mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -e "update data_batch_config set load_data_dt=data_format('$DATA_DT','%Y-%m-%d') where proc_name='proc_work_tsk_rnmd';"
					mysql --defaults-extra-file=/home/ccrm.secret.cnf ccrm -e "update data_batch_config set DATA_INTO_APTAB_TIME_OR_PROC_FIN_TIME=NOW()  where proc_name='proc_work_tsk_rnmd';"
					mysql --defualts-extra-file=/home/ccrm/secret.cnf ccrm -e "update data_batch_config set status='2' where proc_name='proc_work_tsk_rmnd';"
				else
					echo "rc=255,失败,mesage=WORK_TSK_RMND 存储过程执行失败！" | tee -a $LOG_FILE
				fi
			fi
			tables=${tables/$table}
		fi
	done
	sleep 10
done
