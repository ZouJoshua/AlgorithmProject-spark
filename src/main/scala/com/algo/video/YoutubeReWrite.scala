package com.algo.video

import org.apache.spark.sql.SparkSession

/**
  * Created by Joshua on 2019-03-15
  */
object YoutubeReWrite {
  def main(args: Array[String]): Unit = {
    val appName = "Video-YouTube-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext


    //------------------------------------ 重新存储视频文件-----------------------------------------
    // 从Feed流线上数据拉取后选取视频相关字段

    val dt = "2019-03-15"
    val video_path = "/user/zoushuai/news_content/writemongo/video/dt=2019-03-15"
    val video_youtube_path = "video/youtube/dt=%s".format(dt)
    val df = spark.read.parquet(video_path)
    val video_df = df.select("_id", "inner_type", "statistics","country","lang","categories","category","sub_category",
      "third_category", "video_type","sub_class","keywords", "ptime","source","source_url","text","article_title","photos","viewCount",
      "duration","utime","ctime","detail", "resource_type","parent_url","tags","engined_tags","account","gif_link",
      "mp4_link","gifv_link","parent_id", "news_id","source_id","videos","title")

    video_df.write.mode("overwrite").save(video_youtube_path)



  }
}
