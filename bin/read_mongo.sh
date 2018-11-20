#!/usr/bin/env bash

basepath=`readlink -f $0 | xargs dirname | xargs dirname`

dt=$1

if [ x"${dt}" = x ]; then
   dt=`date "+%Y-%m-%d"`
fi

inputurl=$2
if [ x"${inputurl}" = x ]; then
   inputurl="mongodb://10.10.40.122:27017/article_repo.operate_res"
fi

#依赖包路径
libpath=$basepath/lib
#依赖包
DEPEDENCE_JARS=""
for jar in `ls $libpath/*.jar`
do
  DEPEDENCE_JARS=$DEPEDENCE_JARS,$jar
done

#export HADOOP_USER_NAME=atlas
/opt/spark2.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--jars ${DEPEDENCE_JARS#*,} \
--driver-memory 4g \
--num-executors 10 \
--executor-memory 8g \
--executor-cores 4 \
--conf spark.port.maxRetries=1000 \
--conf spark.driver.maxResultSize=6g \
--conf spark.rpc.message.maxSize=2040 \
--conf spark.yarn.executor.memoryOverhead=3g \
--class com.apus.content.ReadMongodb \
${basepath}/lib/mongo_hdfs-1.0.jar \
-date ${dt} -inputUrl ${inputurl}