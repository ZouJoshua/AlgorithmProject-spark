package com.apus.mongodb

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


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
//    val result = ori_df.drop("id","streamingParseTime").join(profile,Seq("resource_id")).withColumn("buss_type", lit(1))
//    result.coalesce(1).write.save("tmp/cms2marktestdata")

    //------------------------------------ 写入mongodb-----------------------------------------
    //

    val dt = "2019-03-08"
    val inputPath = "/user/zoushuai/news_content/cms2marktestdata"
    val outputUri = "mongodb://127.0.0.1:27017/news.article_info_doc"


    val  keywordType = {
      new StructType()
        .add("score",DoubleType)
        .add("word",StringType)
    }
    val algo_profileType = {
      new StructType()
        .add("review_status",ArrayType(IntegerType))
        .add("category",StringType)
        .add("sub_category", StringType)
        .add("status",StringType)
        .add("title_keywords",ArrayType(keywordType))
        .add("content_keywords",ArrayType(keywordType))
    }

    val accountType = {
      new StructType()
        .add("id",LongType)
        .add("name", StringType)
    }

    val originType = {
      new StructType()
        .add("size",DoubleType)
        .add("width",DoubleType)
        .add("url",StringType)
        .add("height",DoubleType)
    }

    val imagesType = {
      new StructType()
        .add("origin",originType)
    }

    val saveDf = {
      spark.read.parquet(inputPath)
        .withColumnRenamed("algo_profile", "algo_profile_old")
        .withColumnRenamed("images","images_old")
        .withColumnRenamed("account","account_old")
        .withColumn("account",from_json(col("account_old"),accountType))
        .withColumn("images",from_json(col("images_old"), imagesType))
        .withColumn("algo_profile",from_json(col("algo_profile_old"),algo_profileType))
        .selectExpr("inner_type","share_link", "rqstid","url","title","country","lang","source","summary","introduction","buss_type",
          "generate_url","account","resource_id","article","algo_profile","images","repeat_content",
          "cast(category as int) category", "cast(sub_class as int) sub_class",
          "cast(sub_category as int) sub_category","cast(third_category as int) third_category",
          "cast(create_time as long) create_time","cast(pub_time as long) pub_time",
          "cast(words as int) words", "cast(image_count as int) image_count",
          "cast(thumbnail_count as int) thumbnail_count","cast(status as int) status",
          "cast(news_type as int) news_type", "cast(latest_interest as int) latest_interest",
          "cast(expire_time as long) expire_time", "cast(resource_type as int) resource_type",
          "cast(is_hotnews as int) is_hotnews","cast(is_newarticle as int) is_newarticle",
          "cast(is_partner as int) is_partner", "cast(origin_category as int) origin_category",
          "cast(source_id as long) source_id")

    }



    print(saveDf.printSchema())
    val num = saveDf.count
    // 写入mongodb
    saveDf.write.options(Map("spark.mongodb.output.uri" -> outputUri))
//      .mode("append")
            .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    println("\nSuccessfully write %1$s data to mongodb".format(num))
    spark.stop()

  }
}
