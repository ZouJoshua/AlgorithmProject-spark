package com.privyalgo.video

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by Joshua on 2019-07-02
  */
object TagsProcess {
  def main(args: Array[String]): Unit = {
    val appName = "Video-Tags-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext

    //------------------------------------ 处理多国tags-----------------------------------------
    // 处理tags
    val tags_path = "/user/hive/warehouse/apus_ai.db/recommend/common/video_trial/dt=2019-07-03"

    val tags_df = {
      spark.read.parquet(tags_path)
        .selectExpr("id","country","lang","article_title","text","business_type","concat_ws(',',tags.name) as vtaglist")
    }

    tags_df.coalesce(1).write.mode("overwrite").format("json").save("video/video_tags")

//    // 整理抓取的tags
//    val tags = youtube_IN_df.select("_id","tags_name").flatMap(r => r.getAs[Seq[String]]("tags_name")).withColumn("tags_name_lower", lower(col("value")))
//    val tags_out = tags.groupBy("value").count.sort(desc("count")).map(r => r.getAs[String]("value") + "\t" + r.getAs[Int]("count").toString).toDF("tags")
//    tags_out.coalesce(1).write.mode("overwrite").format("text").save("video/youtube_tags")
//
//    // 整理已计算的tags
//    val engined_tags = youtube_IN_df.select("_id","engined_tags_name").flatMap(r => r.getAs[Seq[String]]("engined_tags_name")).withColumn("tags_name_lower", lower(col("value")))
//    val engined_tags_out = engined_tags.groupBy("value").count.sort(desc("count")).map(r => r.getAs[String]("value") + "\t" + r.getAs[Int]("count").toString).toDF("tags")
//    engined_tags_out.coalesce(1).write.mode("overwrite").format("text").save("video/youtube_engined_tags")

  }
}
