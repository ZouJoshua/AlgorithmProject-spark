package com.privyalgo.video

import org.apache.spark.sql._
import org.apache.spark.sql.functions._



/**
  * Created by Joshua on 19-6-12
  */
object GameCategory {
  def main(args: Array[String]): Unit = {
    val appName = "GameVideo-Category-Data-Process"
    val spark = SparkSession.builder()
      .master("local")
      .appName(appName)
      .getOrCreate()
    val sc = spark.sparkContext

    val ori_path = "/user/zoushuai/news_content/writemongo/video/dt=2019-06-12/business_type=*"
    val new_path = "video/video_data/dt=2019-06-12"

    val df = spark.read.parquet(ori_path)
    df.write.mode("overwrite").save(new_path)

    df.filter("business_type = 1").filter("category is not null").coalesce(1).write.mode("overwrite").format("json").save("video/game")

  }
}
