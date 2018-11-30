package com.apus.test

import java.text.SimpleDateFormat

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Joshua on 2018-11-06
  */

object ReadMongodb {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("MongoSparkConnectorIntro")
      .getOrCreate()

    val variables = DBConfig.parseArgs(args)
    val currentTimestamp = System.currentTimeMillis()
    val sdf = new  SimpleDateFormat("yyyy-MM-dd")
    val today = sdf.format(currentTimestamp)
    val date = variables.getOrElse("date", today)

    val inputUri = variables.getOrElse("operateResUrl", DBConfig.operateResUrl)
    val outputPath = variables.getOrElse("outpath", DBConfig.operateResSavePath + "/dt=" + date)

    // 从mongodb读取完成标注数据
    val df = spark.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> inputUri,
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"))
      .load()

    val df_list = List("article_doc_id", "article_id", "choose_keywords",
                  "manual_keywords", "is_right", "one_level", "two_level", "three_level",
                  "op_id", "op_name", "op_time", "server_time")

    val originDf = df.select(df_list.map(col): _*)
    val num = originDf.count()

    originDf.repartition(1).write.mode(SaveMode.Overwrite).parquet(outputPath)
    println("\nSuccessfully write %1$s data to hdfs".format(num))
    spark.stop()
  }
}
