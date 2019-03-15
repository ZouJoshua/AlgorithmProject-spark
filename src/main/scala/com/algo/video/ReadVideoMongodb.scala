package com.algo.video

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.algo.mongodb.DBConfig
import org.bson.types.Decimal128


/**
  * Created by Joshua on 2018-11-06
  */

object ReadVideoMongodb {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ReadMongoSparkConnector")
      .getOrCreate()

    val variables = DBConfig.parseArgs(args)
    val date = variables.getOrElse("date", DBConfig.today)

    val inputUri = variables.getOrElse("operate_res_url", DBConfig.operateResUrl)
    val savePath = variables.getOrElse("operate_res_savepath", DBConfig.operateResSavePath)
    val outputPath = savePath + "/dt=%s".format(date)

    // 从mongodb读取完成标注数据
    val df = spark.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> inputUri,
        "spark.mongodb.input.readPreference.name" -> "secondary",
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"))
      .load()
    //all
//    val df_list = List("article_doc_id", "article_id", "choose_keywords",
//                  "manual_keywords", "is_right", "one_level", "two_level", "three_level",
//                  "op_id", "op_name", "op_time", "server_time")
    //test
//    val df_list = List("article_doc_id", "article_id", "choose_keywords",
//                "is_right", "one_level", "two_level", "op_time", "server_time")

//    val originDf = df.select(df_list.map(col): _*)
//    val num = originDf.count()

//    originDf.repartition(1).write.mode(SaveMode.Overwrite).parquet(outputPath)
    println(df.show)
    val out_df = df.filter("resource_type = '20002' or resource_type = '11'")
    val num = out_df.count()
    out_df.write.mode(SaveMode.Overwrite).parquet(outputPath)
    println("\nSuccessfully write %s data to HDFS: %s".format(num, outputPath))
    spark.stop()
  }
}
