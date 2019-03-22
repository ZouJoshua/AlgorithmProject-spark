package com.privyalgo.mongodb

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, TextNode}


/**
  * Created by Joshua on 2019-03-08
  */


object CMS2MarkTestData {


  def main(args: Array[String]): Unit = {
    val appName = "CMSmark2-Test-Data"
    val spark = SparkSession.builder()
      .master("local")
      .appName("ReadMongoSparkConnector")
      .getOrCreate()
    val sc = spark.sparkContext


    //------------------------------------ 处理测试数据-----------------------------------------
    //
//    val profilePath = "news_content/cmsmark2test"
//    val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*"
//    val profile = spark.read.parquet(profilePath).selectExpr("id as resource_id", "algo_profile")
//    val ori_df = spark.read.parquet(newsPath)
//    val result = ori_df.drop("id","streamingParseTime").join(profile,Seq("resource_id")).withColumn("buss_type", lit(0))
//    result.coalesce(1).write.save("tmp/cms2marktestdata")

    //------------------------------------ 写入mongodb-----------------------------------------
    //

    val dt = "2019-03-08"
    val inputPath = "/user/zoushuai/news_content/cms2marktestdata"
    // 开发库
    // val outputUri = "mongodb://127.0.0.1:27017/article_repo.article_info_doc"
    // 测试库
    val outputUri = "mongodb://127.0.0.1:27017/article_repo.article_info_doc"
    val buss_type = 0
//    val df = spark.read.parquet(inputPath).filter("dt = '2019-03-16'").withColumn("buss_type",lit(buss_type)).withColumn("batch_id",lit(2019031801)).drop("dt")
//     val df = spark.read.parquet(inputPath).filter("dt = '2019-03-17'").withColumn("buss_type",lit(buss_type)).withColumn("batch_id",lit(2019031802)).drop("dt")
//    val df = spark.read.parquet(inputPath).filter("dt = '2019-03-18'").withColumn("buss_type",lit(buss_type)).withColumn("batch_id",lit(2019031803)).drop("dt")
    val df = spark.read.parquet(inputPath).filter("dt = '2019-03-19'").sample(false, 0.1).withColumn("buss_type",lit(buss_type)).withColumn("batch_id",lit(2019032201)).drop("dt")
    val cols = Seq("inner_type", "rqstid", "resource_id", "url", "title", "country", "lang", "category", "sub_class", "sub_category", "third_category", "create_time", "pub_time", "source", "source_id", "summary", "words", "introduction", "image_count", "thumbnail_count", "share_link", "article", "status", "news_type", "latest_interest", "extra", "account" , "expire_time", "resource_type", "is_hotnews", "is_newarticle", "is_partner", "generate_url", "origin_category", "algo_profile", "images", "nlp_keywords", "repeat_content", "buss_type","batch_id")
    val saveDf = df.select(cols.map(col):_*)
    val num = saveDf.count()
    saveDf.write.options(Map("spark.mongodb.output.uri" -> outputUri)).mode("append").format("com.mongodb.spark.sql").save()
    println("\nSuccessfully write %1$s data to mongodb".format(num))
    spark.stop()

  }
}
