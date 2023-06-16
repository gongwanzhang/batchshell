#!/bin/bash

# 设置目录
# 执行命令时，需要更改input_dir的路径为文件所在目录
input_dir="/share/data/CCRM/CDFile/fromCODMS/temp"

# 设置log目录
LOG_FILE=/home/ccrm/batch/control-m/logs/load_full_data.log

# 设置数据日期
DT=$(mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -N -e "SELECT PARAM_VAL FROM CCRM.SYS_PARAM_CONFIG WHERE PARAM_ID='dataBatchDate';")
DATA_DT=$(date -d"$DT +1 days" +"%Y%m%d")
DT_DATA=$(date -d"$DT +1 days" +"%Y-%m-%d")

# 数据库
DB_NAME="CCRM"


# 定义数据库连接函数
function mysql_exec(){
	mysql -u$DB_USER -p$DB_PASS -h$DB_HOST $DB_NAME
}


#  删除log文件
 rm -rf $LOG_FILE

# 定义需要全量处理的表
tables_full=$(mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -N -e "SELECT PROC_NAME from CCRM.DATA_BATCH_CONFIG WHERE method='1'")


# 循环检测数据库配置表的LOAD_STATUS状态
while [ -n "$(echo -e "${tables_full}" | tr -d '[:space:]')" ];do
	for table in $tables_full;do
		LOAD_STATUS=$(mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -N -e "SELECT LOAD_STATUS FROM CCRM.DATA_BATCH_CONFIG WHERE PROC_NAME='$table'")
		if [ $LOAD_STATUS == 2 ];then
			# 视图默认指向到A表，并将数据入到B表中
			mysql --defaults-extra-file=/home/ccrm/secret.cnf CCRM -N -e "call PROC_APP_TABLE_SWITCH_A('${DT_DATA}','$table');" >> $LOG_FILE
			if [ $? = 0 ];then
				echo "RC=0,成功,message=$table数据导入B表成功！" | tee -a $LOG_FILE
				mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -e "update data_batch_config set data_info_aptab_time_or_proc_fin_time=now() where PROC_NAME='$table';"
			else
				echo "RC=255,失败，message=$table数据导入B表失败！" | tee -a $LOG_FILE
			fi
			tables_full=${tables_full/$table}
		fi
	done
	sleep 10
done

tables_full=$(mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -N -e "SELECT PROC_NAME from CCRM.DATA_BATCH_CONFIG WHERE method='1'")
for table_name in $tables_full;do
	mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -N -e "call PROC_APP_TABLE_SWITCH_B('${DT_DATA}','$table_name';)" >> $LOG_FILE
	if [ $? = 0 ];then
		echo "RC=0,成功，message=$table_name 视图切换至B表成功！" | tee -a $LOG_FILE
		mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -e "update data_batch_config set load_data_dt=date_format('$DATA_DT', '$Y-$m-%d')" WHERE PROC_NAME='$table_name';
	else
		echo "RC=255,失败，message=$table_name视图切换至B表失败！" | tee -a $LOG_FILE
	fi
done


tables_full=$(mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -N -e "SELECT PROC_NAME from CCRM.DATA_BATCH_CONFIG WHERE method='1'")
for table_A in $tables_full;do
	# 将B表数据同步至A表
	mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -N -e "call PROC_APP_TABLE_SWITCH_C('${DT_DATA}','$table_A');" >> $LOG_FILE
	if [ $? =0 ];then
		echo "RC=0,成功，message=$table_A数据导入A表成功！" | tee -a $LOG_FILE
	else
		echo "RC=255,失败,message=$table_A数据导入A表失败！" | tee -a $LOG_FILE
	fi
done



tables_full=$(mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -N -e "SELECT PROC_NAME from CCRM.DATA_BATCH_CONFIG WHERE method='1'")
for table_B in $tables_full;do
	# 视图切换至A表
	mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -N -e "call PROC_APP_TABLE_SWITCH_D('${DT_DATA}','$table_B');" >> $LOG_FILE
	mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -e "update data_batch_config set status='2' where PROC_NAME='$table_B';"
	if [ $? = 0 ];then
		echo "RC=0,成功,messages=$table_B视图切换至A表成功！" | tee -a $LOG_FILE
	else
		echo "RC=255,失败，message=$table_B视图切换至A表失败！" | tee -a $LOG_FILE
	fi

done

