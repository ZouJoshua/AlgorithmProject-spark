package com.apus.nlp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, Entities, TextNode}
/**
  * Created by Joshua on 2019-01-23
  */
object NewsMarkAllProcess {
  def main(args: Array[String]): Unit = {

    val appName = "News-SubCategory-MarkCorpus-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext


    //------------------------------------ 需标注的文章处理-----------------------------------------
    //
    def mark_process(spark:SparkSession) = {

      val mark_path = ""


    }


  }
}
