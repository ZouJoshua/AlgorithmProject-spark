package com.apus.mark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, TextNode}

/**
  * Created by Joshua on 2019-02-26
  */
object NewsMarchMarkPreProcess {

  def main(args: Array[String]): Unit = {
    val appName = "NewsMarkPreProcesser"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext


    val dt = "2019-02-26"
    val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged"
    val entitywordsPath1 = "/user/zhoutong/tmp/NGramDF_articleID_and_keywords%s".format("/dt=2018-11-2[2-6]")
    val entitywordsPath2 = "/user/hive/warehouse/apus_ai.db/recommend/article/article_profile/dt=2019-01-*"
    val markPath1 = "news_sub_classification/predict/predict_history_unmark_res"
    val markPath2 = "news_sub_classification/predict/predict_lifestyle_world_res"
    val tmpSavePath = "/user/zoushuai/news_content/writemongo/tmp/dt=%s".format(dt)
    val savePath = "/user/zoushuai/news_content/writemongo/March_mark"
    val dupsPath = "news_content/dropdups/dropdups.all_150_5"

    //------------------------------------1 读取600w文章id-----------------------------------------
    //




  }

}
