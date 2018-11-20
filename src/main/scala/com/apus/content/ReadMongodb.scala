package com.apus.content

import java.text.SimpleDateFormat

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Joshua on 2018-11-06
  */

object ReadMongodb {
  def main(args: Array[String]): Unit = {

    val variables = DBConfig.parseArgs(args)
    val currentTimestamp = System.currentTimeMillis()
    val sdf = new  SimpleDateFormat("yyyy-MM-dd")
    val today = sdf.format(currentTimestamp)
    val date = variables.getOrElse("date", today)

    val input = (DBConfig.userName, DBConfig.password) match {
      case (Some(u), Some(pw)) => s"mongodb://$u:$pw@${DBConfig.host}:${DBConfig.port}/${DBConfig.database}.${DBConfig.readCollection}"
      case _ => s"mongodb://${DBConfig.host}:${DBConfig.port}/${DBConfig.database}.${DBConfig.readCollection}"
    }
    val inputUri = variables.getOrElse("inputUrl", input)
//    val inputUri = s"mongodb://${DBConfig.host}:${DBConfig.port}/${DBConfig.database}.${DBConfig.readCollection}"
    val outputPath = DBConfig.writeToHdfsPath + "/dt=" + date

    val spark = SparkSession.builder()
      .appName("MongoSparkConnectorIntro")
      .getOrCreate()

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
//    println(originDf)

    originDf.repartition(1).write.mode(SaveMode.Overwrite).parquet(outputPath)
    println("\nSuccessfully write %1$s data to hdfs".format(num))
    spark.stop()
  }
}
