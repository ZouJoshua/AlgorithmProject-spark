#!/usr/bin/env bash

basepath=`readlink -f $0 | xargs dirname | xargs dirname`

RUNJAR=${basepath}/lib/apus-up-0.2-SNAPSHOT.jar

articleInfoUrl=$1
if [ x"${articleInfoUrl}" = x ]; then
   articleInfoUrl="mongodb://10.10.40.122:27017/article_repo.article_info"
fi

articleInfoHdfsPath=$2
if [ x"${articleInfoHdfsPath}" = x ]; then
   articleInfoHdfsPath="/user/zoushuai/news_content/writemongo"
fi

dt=$3
if [ x"${dt}" = x ]; then
   dt=`date "+%Y-%m-%d"`
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
--class com.privyalgo.mongodb.WriteMongodb \
{RUNJAR} \
-article_info_url ${articleInfoUrl} -article_info_hdfspath ${articleInfoHdfsPath} -date ${dt}