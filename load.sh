#!/bin/bash
BATCH_PATH=/nasdata/batch/$(date +%Y%m%d)
DB_IP=""
DB_NAME=""
DB_USER=""
DB_PWD=""
date=$(date +%Y%m%d)
PORT=""
suffix=".del"
suffixEnd=".end"
MYSQL_ETL="mysql -h${DB_IP} -D${DB_NAME} -u${DB_USER} -p${DB_PWD} --local-infile=1 -s -e"

# 循环查询今日数据是否全部同步成功

while [ 1 -eq 1 ]
do

loopSql="select (select count(1) from sys_tab_load_conf where status='0') - (select count(1) from sys_tab_load_reord where load_data='${date}' and load_status='2') as remain_load_num from dual"
loopCount=$($MYSQL_ET "${loopSql}")

if [ ${loopCount} -eq 0 ]
	then
		break
fi

# 查询出配置中的表名并循环
# 同步状态 0或空-未同步 1-失败 2-成功 关联记录表 去除今日同步成功的表
ssql="SELECT if_tab_name,file_name,tab_col_total.if_increase from sys_tab_load_conf conf
	left join sys_tab_load_reord record on conf.file_name = record.load_file_name and
	record.load_date= '${date}'
	where status = 0 and (load_status is null or load_status='1' or load_status='')"
mkdir -p $BATCH_PATH/$date/temp
mkdir -p $BATCH_PATCH/$date/log
tempFile1=$BATCH_PATH/$date/temp/tempTableConf.txt
tempFile2=$BATCH_PATH/$date/temp/tempColConf.txt
tempFile3=$BATCH_PATH/$date/temp/tempColumnStr
touch ${tempFile1}
touch ${tempFile2}
touch ${tempFile3}

$MYSQL_ETL "${ssql}" > ${tempFile1}

echo "开始循环表配置中配置信息！"

while read line

do echo $line

table_name=`echo $line | awk '{print $1}'`
file_name=`echo $line | awk '{print $2}'`
tab_col_total=`echo $line | awk '{print $3}'`
if_increase=`echo $line | awk '{print $4}'`
logFile=$BATCH_PATH/$date/log/import_${table_name}${date}.log


# 新建临时表
temp_table_name=${table_name}${date}
createSql="CREATE TABLE IF NOT EXISTS ${table_name}${date} LIKE ${table_name}"
createResult=$($MYSQL_ETL "${createSql}")


echo "=====================新建临时表${temp_table_name}成功！" >> ${logFile}

mergeFailRecord="insert into sys_tab_load_reord (load_date,load_status,load_table_name,load_file_name) values ('${date}','1','${table_name}','${file_name}') ON DUPLICATE KEY UPDATE LOAD_STATUS='1'"
mergeSuccRecord="insert into sys_tab_load_reord (load_date,load_status,load_table_name,load_file_name) values ('${date}','2','${table_name}','${file_name}') ON DUPLICATE KEY UPDATE LOAD_STATUS='2'"


if [ ! -f $BATCH_PATH/${file_name}${date}${suffix} ]
	then
		echo "===================数据文件$BATCH_PATH/${file_name}${date}${suffix}不存在" >> ${logFile}
		echo "RC=151,数据文件$BATCH_PATH/${file_name}${date}${suffix}不存在"
		# 修改或新增今日同步状态为失败
		$($MYSQL_ETL "${mergeFailRecord}")
fi

if [ ! -f $BATCH_PATH/${file_name}${date}${suffixEnd} ]
	then
	echo "RC=151,数据上传完成标志文件$BATCH_PATH/${file_name}${date}${suffixEnd}不存在"
	echo
	"=====================数据上传完成标识文件$BATCH_PATH/${file_name}${date}${suffixEnd}不存在" >> ${logFile}

	# 修改或新增今日同步状态为失败
	${$MYSQL_ETL "${mergeFailRecord}"} 
fi

if [ ! -s $BATCH_PATCH/${file_name}${date}${suffix} ]
	then
		echo "==========================数据文件$BATCH_PATH/${file_name}${date}${suffix}为空" >> ${logFile}
		echo "RC=151,数据文件$BATCH_PATH/${file_name}${date}${suffix}为空"
		# 修改或新增今日同步状态为失败
		${$MYSQL_ETL "${mergeFailRecord}"}
fi




echo "=================开始导入数据文件data ${file_name}${date}${suffix}" >> ${logFile}



# 查询出配置中的列信息并循环取出所有表字段拼接到列信息中
colConfSql="SELECT column_name,column_id from sys_tab_col_conf where table_name='${table_name}' and status='0' order by column_id asc"
unInsertCol=@dummy
$(> ${tempFile3})

$MYSQL_ETL "${colConfSql}" > ${tempFile2}
echo -n '(' >> ${tempFile3}
arr=()
while read line
do
echo $line
column_name=`echo $line | awk '{print $1}'`
column_id=`echo $line | awk '{print $2}'`

	for ((i=1;i<=$ab_col_total;i++))
	do
		if [ -z "${arr[i]}" ]
			then
			arr[i]=${unInsertCol}
		fi
		if [ $i -eq $column_id ]
			then
			echo ${column_name}
			arr[i]=${column_name}
		fi
	done

done < ${tempFile2}
echo ${arr[*]}


# 将字符串最后的恶一个逗号去掉


for i in ${arr[@]}
	do
		echo -n $str$i, >> ${tempFile3}
	done
echo $str

# str取temp文本里的字符串
columnStr=$(cat ${tempFile3})
columnStr=${columnStr%*,}
columnStr="$columnStr"



echo "==========================${temp_table_name} 将导入列字段:${columnStr}" >> ${logFile}


loadPath=$BATCH_PATH/${file_name}${date}${suffix}

if [ $if_increase -eq 0 ]
	then
		# 增量数据直接加载数据文件到正式表中
		loadFormalSql="load data local infile '${loadPath}' ignore into table ${table_name} character set utf8 fields terminated by '|' LINES TERMINATED BY '\\n' ${columnStr}"
		$($MYSQL_ETL "${loadFormalSql}")
		echo "===============增量同步正式表${table_name}数据成功！" >> ${logFile}
		# 修改今日同步状态为成功
		$($MYSQL_ETL "${mergeSuccRecord}")
	else
		# 将数据导入到临时表
		loadSql="load data local infile '${loadPath}' ignore into table ${temp_table_name} character set utf8 fields terminated by '|' lines terminated by '\\n' ${columnStr}"

		$($MYSQL_ETL "${loadSql}")

		echo "====================数据文件导入到临时表${temp_table_name}成功！" >> ${logFile}

		# 新建备份表
		createBackupTablSql="create table if not exists ${table_name}_backup like ${table_name}"
		$($MYSQL_ETL "${createBackupTabSql}")
		echo "===================创建备份表${table_name}_backup成功！" >> ${logFile}

		# 删除之前的备份表数据并备份正式表数据到备份表
		
		dropBackupSql="truncate table ${table_name}_backup "
		$($MYSQL_ETL "${dropBackupSql}")
		echo "==============删除备份表${table_name}_backup历史数据成功！" >> ${logFile}



		backupSql="insert into ${table_name}_backup select * from ${table_name}"
		$($MYSQL_ETL "${backupSql}")

		echo "===================备份正式表${table_name}数据到备份表${table_name}_backup成功！" >> ${logFile}

		# 删除正式表数据
		truncateSql="truncate table ${table_name}"
		$($MYSQL_ETL "${truncateSql}")
		echo "======================删除正式表${table_name}历史数据成功！" >> ${logFile}


		# 同步临时表数据到正式表
		asyncSql="insert into ${table_name} select * fro ${temp_table_name}"
		$($MYSQL_ETL "${asyncSql}")

		echo "=====================同步临时表${temp_table_name}数据到正式表${table_name}成功!" >> ${logFile}

		# 删除临时表
		dropTempTableSql="drop table ${temp_table_name}"
		$($MYSQL_ETL "${dropTempTableSql}")
		echo "====================删除临时表${tmep_table_nbame}成功！" >> ${logFile}

		# 修改今日同步状态为成功
		$($MYSQL_ETL "${mergeSuccRecord}")

fi



done < ${tempFile1}


done


echo "RC=0,Message=数据文件导入成功"

exit 0
