package com.privyalgo.video

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Created by Joshua on 2019-03-15
  */
object YoutubeReWrite {
  def main(args: Array[String]): Unit = {
    val appName = "Video-YouTube-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext


    //------------------------------------ 重新存储视频文件(20190407)-----------------------------------------
    // 从Feed流线上数据拉取后选取视频相关字段

//    val dt = "2019-03-15"
    val dt = "2019-04-02"
    val video_path = "/user/zoushuai/news_content/writemongo/video/dt=2019-04-07"
    val video_youtube_path = "video/youtube/dt=%s".format(dt)
    val video_nonyoutube_path = "video/nonyoutube/dt=%s".format(dt)
    val df = spark.read.parquet(video_path)

    val nlp_keywordType = {
      new StructType()
        .add("keyword",StringType)
        .add("weight",IntegerType)
    }



    val nlp_udf = udf((t:Seq[String]) => if(t!=null&&t.nonEmpty) "["+ t.mkString(",")+ "]" else "")

    // 视频数据
    val video_df = {
      df.withColumn("nlp_keywords_old", nlp_udf(col("nlp_keywords")))
        .withColumn("nlp_keywords_new", when(col("nlp_keywords_old").isNotNull,from_json(col("nlp_keywords_old"),ArrayType(nlp_keywordType))))
        .selectExpr("_id", "inner_type", "statistics","country","lang","categories","category","sub_category",
        "third_category", "video_type","sub_class","keywords", "ptime","source","source_url","text","article_title","photos","viewCount",
        "duration","utime","ctime","detail", "resource_type","parent_url","tags","engined_tags","account","gif_link","review_status","id",
        "mp4_link","gifv_link","parent_id", "news_id","source_id","videos","title","rqstid","nlp_keywords_new as nlp_keywords","is_partner","is_hotvideos","expire_time")
        .withColumn("status",lit(1))
    }
    video_df.write.mode("overwrite").save("video/video_data/dt=2019-04-02")

    // youtube视频数据
    val youtube_video_df = {
      video_df.filter("resource_type in (20002,11)")
    }

    youtube_video_df.coalesce(10).write.format("json").mode("overwrite").save(video_youtube_path)

    // 非youtube视频数据
    val nonyoutube_video_df = {
      video_df.filter("resource_type not in (20002,11)")
    }
    nonyoutube_video_df.coalesce(1).write.format("json").mode("overwrite").save(video_nonyoutube_path)

  }
}
