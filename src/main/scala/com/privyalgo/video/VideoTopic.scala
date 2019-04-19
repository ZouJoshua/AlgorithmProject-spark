package com.privyalgo.video

/**
  * Created by Joshua on 19-4-18
  */


import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object VideoTopic {

  def main(args: Array[String]): Unit = {
    val appName = "Video-Topic-Data-Process"
    val spark = SparkSession.builder()
      .master("local")
      .appName(appName)
      .getOrCreate()
    val sc = spark.sparkContext


    val vPath = "video/video_data/dt=2019-04-02"
    val vOutPath = "video/v_topic"

    val vDF = {
      spark.read.parquet(vPath)
        .filter("country = 'IN' and lang= 'en'")
        .select("id","text","article_title","resource_type", "source_url")
        .filter("text != '' and article_title != ''")
    }
    vDF.coalesce(1).write.format("json").mode("overwrite").save(vOutPath)

    spark.stop()
  }
}
