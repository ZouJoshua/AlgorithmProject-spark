package com.privyalgo.nlp

/**
  * Created by Joshua on 19-4-11
  */

import javassist.bytecode.SignatureAttribute
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object NewsSchemaParser {
  def main(args: Array[String]): Unit = {

    val _appName = "IN-Hi-News-Parser"
    val spark = SparkSession.builder()
      .master("local")
      .appName(_appName)
      .getOrCreate()
    val sc = spark.sparkContext


    //***********//
    //印度语格式解析//
    //***********//

    val newsPath = "/user/zhoutong/news_hindi_data"
    val outPath = "/user/zoushuai/news_content/news_hindi_all"

    val ArticleType = {
      new StructType()
        .add("html",StringType)
        .add("tags", ArrayType(StringType))
    }

    val image_originType = {
      new StructType()
        .add("size",IntegerType)
        .add("width",IntegerType)
        .add("url",StringType)
        .add("height",IntegerType)
    }

    val ImagesType = {
      new StructType()
        .add("origin",image_originType)
        .add("detail_large",image_originType)
        .add("list_large",image_originType)
        .add("list_small",image_originType)
    }

    val ShareLinkType = {
      new StructType()
        .add("1", StringType)
        .add("2", StringType)
        .add("4", StringType)
        .add("10", StringType)
    }

    val ExtraType = {
      new StructType()
        .add("id",IntegerType)
        .add("image_source", ArrayType(StringType))
    }

    val newsSchema ={
      new StructType()
        .add("lang",StringType)
        .add("category", IntegerType)
        .add("pub_time", LongType)
        .add("resource_id",LongType)
        .add("url",StringType)
        .add("country",StringType)
        .add("title",StringType)
        .add("summary",StringType)
        .add("source",StringType)
        .add("create_time",LongType)
        .add("status",IntegerType)
        .add("latest_interest",IntegerType)
        .add("article",ArticleType)
        .add("images",ArrayType(ImagesType))
        .add("news_type",IntegerType)
        .add("extra",ExtraType)
        .add("share_link",ShareLinkType)
    }


    //  方法1
    val newsParsedDf = {
      spark.read.parquet(newsPath)
        .withColumn("value",from_json(col("value"),newsSchema))
        .select("value.*","dt")
    }
    newsParsedDf.write.mode("overwrite").save(outPath)  // 路径移至 /user/hive/warehouse/apus_ai.db/recommend/common/news_hindi_parsed/


    //  方法2.（解析出的都是字符串，还需要转化类型）
//    val newsParsedDf = {
//      spark.read.parquet(newsPath).select(
//        get_json_object(col("value"),"$.lang").alias("lang"),
//        get_json_object(col("value"),"$.category").alias("category"),
//        get_json_object(col("value"),"$.pub_time").alias("pub_time"),
//        get_json_object(col("value"),"$.resource_id").alias("resource_id"),
//        get_json_object(col("value"),"$.url").alias("url"),
//        get_json_object(col("value"),"$.country").alias("country"),
//        get_json_object(col("value"),"$.title").alias("title"),
//        get_json_object(col("value"),"$.summary").alias("summary"),
//        get_json_object(col("value"),"$.source").alias("source"),
//        get_json_object(col("value"),"$.create_time").alias("create_time"),
//        get_json_object(col("value"),"$.status").alias("status"),
//        get_json_object(col("value"),"$.latest_interest").alias("latest_interest"),
//        get_json_object(col("value"),"$.article").alias("article"),
//        get_json_object(col("value"),"$.images").alias("images"),
//        get_json_object(col("value"),"$.status").alias("status"),
//        get_json_object(col("value"),"$.latest_interest").alias("latest_interest"),
//        get_json_object(col("value"),"$.share_link").alias("share_link")
//      )
//    }



  }
}
