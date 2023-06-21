#!/usr/bin/ksh


. ${ENVDIR}/global_para.env
. ${ENVDIR}/global_func.env

file_name=$1
dt=$2
filename=`echo ${file_name} | awk -F. '{print $1}'`
tablename=ss
loadtype=ss
importorload=ss
cleartype=ss
fdir=ss
etl_dt=""


# 检验文本字段与数据库字段是否相符
check()
{
	if [ X"${packname}" = X"ODS" ];then
		if [ -f ${loaddir}/${dt}/${filename}${dt}.del ]
		then
			fdir=`ls ${loaddir}/${dt}/${filename}${dt}.del`
		else
		
}
