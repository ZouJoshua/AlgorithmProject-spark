package com.privyalgo.video

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by Joshua on 2019-03-15
  */
object YoutubeTagsProcess {
  def main(args: Array[String]): Unit = {
    val appName = "Video-YouTube-Tags-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext

    //------------------------------------ 处理youtube的tags-----------------------------------------
    // 处理印度英语的tags

      val categoryMap = Map(
        1->"Hot",
        2->"Music",
        3->"Movies",
        4->"Sports",
        5->"News",
        6->"Comedy",
        7->"Tech",
        8->"TVShows",
        9->"Football",
        10->"Entertainment",
        11->"Lifestyle",
        12->"Kids",
        14->"Cartoon",
        15->"Spirituality",
        16->"Game",
        200->"Funny",
        201->"GIF",
        202->"Food",
        203->"Viral",
        204->"Inspirational",
        205->"Tutorial（fashion）",
        206->"Animals",
        207->"Offbeat",
        208->"Ramadan"
      )

    val category2StrUDF = udf{(category:Int) => categoryMap.getOrElse(category, "others")}

    val youtube_IN_df = {
      spark.read.parquet("video/youtube/dt=2019-03-15")
        .filter("country = 'IN' and lang= 'en'")
        .withColumn("category_str", category2StrUDF(col("category")))
        .selectExpr("_id","country","lang","article_title","text","source_url","keywords","category_str as category","sub_category","tags.name as tags_name","engined_tags.name as engined_tags_name")
    }

    youtube_IN_df.coalesce(1).write.mode("overwrite").format("json").save("video/youtube_detail")

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
