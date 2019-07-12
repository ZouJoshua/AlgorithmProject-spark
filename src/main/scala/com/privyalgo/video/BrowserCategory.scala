package com.privyalgo.video

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * Created by Joshua on 19-6-12
  */
object BrowserCategory {
  def main(args: Array[String]): Unit = {
    val appName = "BrowserVideo-Category-Data-Process"
    val spark = SparkSession.builder()
      .master("local")
      .appName(appName)
      .getOrCreate()
    val sc = spark.sparkContext

    val ori_path = "/user/hive/warehouse/apus_ai.db/recommend/common/video_trial/dt=2019-07-11/business_type=0"

    val df = {
      spark.read.parquet(ori_path)
        .filter("category in ('211','212','213','214','215','216','217'" +
        "'218','219','220','221','222','223','224','225','226','227','228','229','230')")
        .select("id","country","lang","text","article_title",
          "source_url","category","sub_category","third_category")
    }

    val review_df = {
      spark.read.parquet(ori_path)
        .filter("category in ('211','212','213','214','215','216','217'" +
          "'218','219','220','221','222','223','224','225','226','227','228','229','230')")
        .filter("r_category != -1")
        .filter("category != r_category")
        .filter("r_category in ('211','212','213','214','215','216','217'" +
          "'218','219','220','221','222','223','224','225','226','227','228','229','230')")
        .selectExpr("id","country","lang","text","article_title",
        "source_url","r_category as category","r_sub_category as sub_category",
        "r_third_category as third_category")
        .withColumn("mark",lit(1))
    }
    val right_df = df.join(review_df.select("id","mark"),Seq("id"),"left").filter("mark is null").drop("mark")
    val all = right_df.union(review_df.drop("mark"))

    all.coalesce(1).write.mode("overwrite").format("json").save("video/browser")

  }
}
