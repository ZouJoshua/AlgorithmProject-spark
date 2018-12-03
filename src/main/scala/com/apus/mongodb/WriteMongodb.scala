package com.apus.mongodb

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by Joshua on 2018-11-07
  */

object WriteMongodb {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("WriteMongoSparkConnector")
      .getOrCreate()

    val variables = DBConfig.parseArgs(args)
    val currentTimestamp = System.currentTimeMillis()


    val dt = variables.getOrElse("date",DBConfig.today)
    val input = variables.getOrElse("article_info_hdfspath", DBConfig.writeArticleInfoPath)
    val inputPath = input + "/dt=%s".format(dt)
    val outputUri = variables.getOrElse("article_info_url", DBConfig.articleInfoUrl).toString

    // 读取已清洗好的数据
    val df = spark.read.parquet(inputPath)

    // 选取写入mongodb的字段
    val colnames = Seq("article_id", "country_lan", "one_level", "two_level", "three_level",
                "need_double_check", "mark_level", "article_url", "title", "article",
                "entity_keywords", "semantic_keywords")

    val saveDf = df.select(colnames.map(col):_*).withColumn("create_time", lit(currentTimestamp))
                    .withColumn("do_grab", lit(0))
    val num = saveDf.count()
    // 写入mongodb
    saveDf.write.options(Map("spark.mongodb.output.uri" -> outputUri))
      .mode("append")
      .format("com.mongodb.spark.sql")
      .save()
    println("\nSuccessfully write %1$s data to mongodb".format(num))
    spark.stop()
  }
}
