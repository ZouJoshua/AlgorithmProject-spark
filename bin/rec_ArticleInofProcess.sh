#!/usr/bin/env bash

basepath=`readlink -f $0 | xargs dirname | xargs dirname`
RUNJAR=${basepath}/lib/AlgorithmProject-spark-1.0.jar

dt=$1
if [ x"${dt}" = x ]; then
   dt=`date "+%Y-%m-%d"`
fi

entityCategoryPath=$2
if [ x"${entityCategoryPath}" = x ]; then
   entityCategoryPath="/user/zhoutong/tmp/entity_and_category"
fi

unclassifiedPath=$3
if [ x"${unclassifiedPath}" = x ]; then
   unclassifiedPath="/user/caifuli/news/tmp/unclassified"
fi

articleInfoHdfsPath=$4
if [ x"${articleInfoHdfsPath}" = x ]; then
   articleInfoHdfsPath="/user/zoushuai/news_content/writemongo"
fi

marklevel=$5

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
--class com.apus.mongodb.ArticleInfoProcess \
${RUNJAR} \
-date ${dt} \
-entity_category_path ${entityCategoryPath} \
-unclassified_path ${unclassifiedPath} \
-article_info_hdfspath ${articleInfoHdfsPath} \
-mark_level ${marklevel}