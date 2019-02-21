package com.apus.mark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.jsoup.Jsoup

/**
  * Created by Joshua on 2019-02-21
  */
object NewsSubCategoryMarchTrainProcess {
  def main(args: Array[String]): Unit = {

    val appName = "News-SubCategory-March-MarkedCorpus-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext


    val dt = "2019-02-21"
    val newsPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"

    //------------------------------------1 更新国内二级分类 -----------------------------------------
    // 1.去除three_level 为null
    // 2.抽取three_level 为crime、law
    // 3.抽取two_level 为politics、education
    // 4.其他放为others(量不足)

    def update_national_process(spark: SparkSession,
                                   newsPath: String="/user/hive/warehouse/apus_dw.db/dw_news_data_hour",
                                   dt: String="2019-02-21") = {


    }



  }
}
