package com.algo.mark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.jsoup.Jsoup

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
    val markedPath = "/user/caifuli/news/all_data"

    // 查看导入数据
    val writeMarkedPath = "/user/zoushuai/news_content/writemongo/completed_write_march/dt=2019-02-27"

    //------------------------------------1 读取600w文章id-----------------------------------------
    //
    val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
    val ori_df = {
      spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*")
        .selectExpr("resource_id as article_id", "article", "title", "url")
        .dropDuplicates("article_id")
    }

    //------------------------------------2 读取150w已标注文章id-----------------------------------------
    //
    val marked_df = spark.read.json(markedPath)
    val transform_name = marked_df.select(col("news_id").cast(StringType),col("resource_id"))
    val marked_id = marked_df.withColumn("article_id", concat_ws("", transform_name.schema.fieldNames.map(col): _*)).select("article_id").distinct()

    //------------------------------------3 读取已导入文章id-----------------------------------------
    //
    val mongo_id = spark.read.parquet(writeMarkedPath).select("article_id").distinct()

    val all_marked_ids = marked_id.union(mongo_id).withColumn("drop",lit(1)).distinct()

    //------------------------------------4 未标注数据-----------------------------------------

    val unmark_data = {
      ori_df.join(all_marked_ids, Seq("article_id"), "left")
        .filter("drop is null")
        .withColumn("content", getcontentUDF(col("article.html")))
        .drop("article","drop")
        .dropDuplicates("article_id","title","url")
    }
    unmark_data.coalesce(5).write.format("json").mode("overwrite").save("news_sub_classification/predict/predict_march_mark")

  }

}
