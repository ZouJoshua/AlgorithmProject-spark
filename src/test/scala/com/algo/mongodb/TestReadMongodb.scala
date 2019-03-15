package com.algo.mongodb

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
/**
  * Created by Joshua on 2018-12-06
  */


object TestReadMongodb {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("ReadMongoSparkConnector")
      .getOrCreate()
    val dt = "2018-12-06"
    val savePath = "/user/zoushuai/news_content/read_mongo_test"
    val outputPath = savePath + "/dt=%s".format(dt)
    val inputUri = "mongodb://127.0.0.1:27017/news.operate_res"
    val df = spark.read.format("com.mongodb.spark.sql.DefaultSource").options(
      Map("spark.mongodb.input.uri" -> inputUri,
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32")).load()
    // 目前数据库读取列名
    val df_list = List("article_id", "choose_keywords",
      "is_right", "one_level", "two_level",
      "op_time", "server_time")
    val originDf = df.select(df_list.map(col): _*)
    println(originDf.show(false))
    val num = originDf.count()
    print(num)
    originDf.repartition(1).write.mode(SaveMode.Overwrite).parquet(outputPath)
  }
}
