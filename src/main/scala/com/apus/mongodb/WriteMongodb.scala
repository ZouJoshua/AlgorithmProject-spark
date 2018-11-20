package com.apus.mongodb

import java.text.SimpleDateFormat

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by Joshua on 2018-11-07
  */

object WriteMongodb {
  def main(args: Array[String]): Unit = {
    val variables = DBConfig.parseArgs(args)

    val currentTimestamp = System.currentTimeMillis()
    val sdf = new  SimpleDateFormat("yyyy-MM-dd")
    val today = sdf.format(currentTimestamp)

    val inputPath = variables.getOrElse("hdfspath", DBConfig.readFromHdfsPath)
//    val outputUri = s"mongodb://${DBConfig.host}:${DBConfig.port}/${DBConfig.database}.${DBConfig.writeCollection}"

    val output = (DBConfig.userName, DBConfig.password) match {
      case (Some(u), Some(pw)) => s"mongodb://$u:$pw@${DBConfig.host}:${DBConfig.port}/${DBConfig.database}.${DBConfig.writeCollection}"
      case _ => s"mongodb://${DBConfig.host}:${DBConfig.port}/${DBConfig.database}.${DBConfig.writeCollection}"
    }
    val outputUri = variables.getOrElse("outputUrl", output).toString

    val spark = SparkSession.builder()
      .appName("WriteMongoSparkConnectorIntro")
      .getOrCreate()

    val df = spark.read.parquet(inputPath)

    val cols = Seq("article_id", "country_lan", "one_level", "two_level", "three_level",
                "need_double_check", "mark_level", "article_url", "title", "article",
                "entity_keywords", "semantic_keywords")

    val saveDf = df.withColumn("create_time", lit(currentTimestamp))
                    .withColumn("do_grab", lit(0))
    val num = saveDf.count()
    saveDf.write.options(Map("spark.mongodb.output.uri" -> outputUri))
      .mode("append")
      .format("com.mongodb.spark.sql")
      .save()
    println("\nSuccessfully write %1$s data to mongodb".format(num))
    spark.stop()
  }
}
