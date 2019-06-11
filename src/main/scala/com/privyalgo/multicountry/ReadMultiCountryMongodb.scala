package com.privyalgo.multicountry

import com.privyalgo.mongodb.DBConfig
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by Joshua on 2019-06-04
  */

object ReadMultiCountryMongodb {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Read-Multi-Country-Mongo-Spark-Connector")
      .getOrCreate()

    val variables = DBConfig.parseArgs(args)
    val date = variables.getOrElse("date", DBConfig.today)

    val inputUri = variables.getOrElse("operate_res_url", DBConfig.operateResUrl)
    val savePath = variables.getOrElse("operate_res_savepath", DBConfig.operateResSavePath)
    val outputPath = savePath + "/dt=%s".format(date)

    // 从mongodb读取完成标注数据 Map("mergeSchema" -> "true")
    val ori_df = spark.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> inputUri,
        "spark.mongodb.input.readPreference.name" -> "secondary",
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"))
      .load()
    val df = {
      ori_df.select("_id","resource_id","country","lang","category",
        "sub_class","create_time","pub_time","title","url","source",
        "source_id","summary","article","status","news_type",
        "latest_interest","resource_type")
    }
    // ZA->南非，US-> 美国，SG-> 新加坡，CA-> 加拿大， SA->沙特，GB-> 英国，PH->菲律宾
    val us_df = df.filter("inner_type='news'").filter("country='US'").filter("lang='en'")
    us_df.printSchema()
//    us_df.show(1)
    val num1 = us_df.count()
    val out_path1 = outputPath + "/country=%s".format("US")
    us_df.write.mode(SaveMode.Overwrite).save(out_path1)
    println("\nSuccessfully write %s data to HDFS: %s".format(num1, out_path1))
    val gb_df = df.filter("inner_type='news'").filter("country='GB'").filter("lang='en'")
    val num2 = gb_df.count()
    val out_path2 = outputPath + "/country=%s".format("GB")
    gb_df.write.mode(SaveMode.Overwrite).save(out_path2)
    println("\nSuccessfully write %s data to HDFS: %s".format(num2, out_path2))
    val ca_df = df.filter("inner_type='news'").filter("country='CA'").filter("lang='en'")
    val num3 = ca_df.count()
    val out_path3 = outputPath + "/country=%s".format("CA")
    ca_df.write.mode(SaveMode.Overwrite).save(out_path3)
    println("\nSuccessfully write %s data to HDFS: %s".format(num3, out_path3))

    val sa_df = df.filter("inner_type='news'").filter("country='SA'").filter("lang='en'")
    val num4 = sa_df.count()
    val out_path4 = outputPath + "/country=%s".format("SA")
    sa_df.write.mode(SaveMode.Overwrite).save(out_path4)
    println("\nSuccessfully write %s data to HDFS: %s".format(num4, out_path4))

    val za_df = df.filter("inner_type='news'").filter("country='ZA'").filter("lang='en'")
    val num5 = za_df.count()
    val out_path5 = outputPath + "/country=%s".format("ZA")
    za_df.write.mode(SaveMode.Overwrite).save(out_path5)
    println("\nSuccessfully write %s data to HDFS: %s".format(num5, out_path5))

    val ph_df = df.filter("inner_type='news'").filter("country='PH'").filter("lang='en'")
    val num6 = ph_df.count()
    val out_path6 = outputPath + "/country=%s".format("PH")
    ph_df.write.mode(SaveMode.Overwrite).save(out_path6)
    println("\nSuccessfully write %s data to HDFS: %s".format(num6, out_path6))

    spark.stop()
  }
}
