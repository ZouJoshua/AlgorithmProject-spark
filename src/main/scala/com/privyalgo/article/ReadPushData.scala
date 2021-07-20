package com.privyalgo.article

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ReadPushData {

    def main(args: Array[String]): Unit = {
        val appName = "Read-Push-Data"
        val spark = SparkSession.builder()
          .master("local")
          .appName(appName)
          .getOrCreate()
        val sc = spark.sparkContext

        val basic_table_path = "/region04/27367/app/product/feeds_news_wide/date=202010*/scene_p=browser_push/*"
        val value_df = spark.read.text(basic_table_path)

        val value_row_df = value_df.withColumn("splitcol", split(col("value"), "\u0001")).select(
            col("splitcol").getItem(0).as("article_id"),
            col("splitcol").getItem(1).as("title"),
            col("splitcol").getItem(2).as("content"),
            col("splitcol").getItem(3).as("url"),
            col("splitcol").getItem(4).as("source"),
            col("splitcol").getItem(5).as("channel"),
            col("splitcol").getItem(6).as("author"),
            col("splitcol").getItem(7).as("publishtime"),
            col("splitcol").getItem(8).as("type"),
            col("splitcol").getItem(9).as("rawotherinfo"),
            col("splitcol").getItem(10).as("alginfo")
        ).drop("splitcol").dropDuplicates("article_id").distinct().cache

        val output_path = "/region04/27367/app/develop/zs/browser_push_10"
//        val output_path = "/region04/27367/app/develop/zs/browser_push_9"

        value_row_df.coalesce(2).write.format("json").mode("overwrite").save(output_path)


    }
}
