#!/bin/bash
# 功能：mysql数据库跑批脚本


# 设置目录
# 执行命令时，需要更改input_dir路径为文件所在目录
input_dir="/share/data/CCRM/CDFile/fromCODMS/temp"

# 设置log目录
LOG_FILE=/home/ccrm/batch/control-m/logs/load_data.log


# 设置数据日期
DT=$(mysql --defaults-extra-file=/home/ccrm/secret.cnf -N -e "SELECT PARAM_VAL from CCRM.SYS_PARAM_CONFIG WHERE PARAM_ID='dataBatchDate';")
DATA_DT=$(date -d"$DT +1 days" +"%Y%m%d")
DT_DATA=$(date -d"$DT +1 days" +"%Y-%m-%d")

# 数据库
DB_NAME="CCRM"


# 定义数据库连接函数
function mysql_exec(){
	mysql -u$DB_USER -p$DB_PASS -h$DB_HOST $DB_NAME
}


# 跑批开始，修改sys_param_config参数
mysql --defaults-extra-file=/home/ccrm/secret.cnf --local-infile << EOF
UPDATE CCRM.data_batch_config set load_status='0';
UPDATE CCRM.data_batch_config set status='0';
EOF

# CCRM自处理的存储程序
sh /home/ccrm/batch/control-m/load_inchag_data_sys_knwlge_lib.sh &

# 跑批开始，调起各个子任务
# 需要全量跑批的表的shell任务，后台执行
 sh /home/ccrm/batch/control-m/load_full_data.sh &

# 增量跑批的子任务，后台执行
 sh /home/ccrm/batch/control-m/load_incre_data.sh &

# es数据加载
sh /home/ccrm/batch/control-m/load_es.sh &


# 定义需要跑批的表信息
tables=$(mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -N -e "SELECT PROC_NAME FROM ccrm.data_batch_config where method in ("1","2","3");")

# 删除原log
rm -rf $LOG_FILE

# 每隔10秒检查一次，直到所有的.end文件都被处理完毕
while [ -n "$(echo -e "${tables}" | tr -d '[:space:]')" ];do
	for table in $tables;do
		# 定义检测文件以及数据文件
		END_FILE="$input_dir/${table}${DATA_DT}.end"
		DEL_FILE="$input_dir/${table}${DATA_DT}.del"

		# 判断end是否存在
		if [ -f "$END_FILE" ];then
			mysql --defaults-extra-file=/home/ccrm/secret.cnf --local-infile <<EOF >> $LOG_FILE
UPDATE ccrm.data_batch_config set DATA_ARRIVE_TIME_OR_PROC_START_TIME=NOEW() where PROC_NAME='$TABLE';
TRUNCATE TABLE $DB_NAME.${table}_from_dm;
LOAD DATA LOCAL INFILE '$DEL_FILE' INTO TABLE $DB_NAME.${table}_from_dm FIELDS TERMIATED BY '|-@|' ESCAPED BY '^';
SHOW WARNINGS;
EOF

			if [ $? = 0 ];then
				echo "RC=0,成功,message=$table数据入库成功！" | tee -a $LOG_FILE
				# 修改参数为执行成功
				mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -e "update data_batch_config set load_status='2',load_data_dt='$DATA_DT' WHERE PROC_NAME='$table';"
				mysql --defaults-extra-file=/home/ccrm/secret.cnf ccrm -e "update data_batch_config set DATA_INTO_FMTAL_TIME=NOEW() WHERE PROC_NAME='$table';"
			else
				echo "RC=255,失败，message=$table数据入库失败！" | tee -a $LOG_FILE
			fi
			tables=${tables/$table}
		fi
	done
	sleep 10
done


# 等待所有后台任务完成，并记录退出状态
wait_statuses=()
for job in $(jobs -p);do
	wait $job
	wait_statuses+=($?)
done

# 检查每个子shell的退出状态是否都是0，如果是则打印“success”，否则打印错误信息
 forstatus in ${wait_statuses[@]};do
	if [[ $status -ne 0 ]];then
		echo "Some child processes failed with status code $status"
		exit 1
	fi
done


# 所有子shell任务执行完成，修改sys_param_config参数
mysql --defaults-extra-file=/home/ccrm/secret.cnf --local-infile <<EOF
UPDATE CCRM.SYS_PARAM_CONFIG SET PARAM_VAL=NOW() WHERE PARAM_ID='dataRunStopDate';
UPDATE CCRM.SYS_PARAM_CONFIG SET PARAM_VAL='0' WHERE PARAM_ID=IsDataRun';
UPDATE CCRM.SYS_PARAM_CONFIG SET PARAM_VAL=date_format('$DATA_DT','%Y-%m-%d') WHERE PARAM_ID='dataBatchDate';

# 触发跑批接口
 curl -X POST http://10.93.33.223:8086/api/auth/gxinfoData -H 'Content-Type: application/json' -d '{}' &
 curl -X POST http://10.93.33.223:8086/api/auth/timeData -H 'Content-Type: application/json' -d '{}' &


# 判断跑批接口是否陈宫
 if [ $? = 0 ];then
	echo "RC=O,成功，message=跑批接口触发成功！" | tee -a $LOG_FILE
 else
	echo "RC=255,失败，message=跑批接口触发失败！" | tee -a $LOG_FILE
 fi

