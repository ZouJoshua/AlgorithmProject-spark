package com.privyalgo.article

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ReadPushClickData {

    def main(args: Array[String]): Unit = {
        val appName = "Read-Push-Click-Data"
        val spark = SparkSession.builder()
          .master("local")
          .appName(appName)
          .getOrCreate()
        val sc = spark.sparkContext

        // article info
        // scene_p = [search_term,browser_push,recommend,crawl]
        val article_info_path = "/region04/27367/app/product/feeds_news_wide/date=202012*/scene_p=browser_push/*"
        val article_detail_info_df = spark.read.text(article_info_path)
        val value_row_df = article_detail_info_df.withColumn("splitcol", split(col("value"), "\u0001")).select(
            col("splitcol").getItem(0).as("article_id"),
            col("splitcol").getItem(1).as("title"),
            col("splitcol").getItem(2).as("content"),
            col("splitcol").getItem(3).as("url"),
            col("splitcol").getItem(4).as("source")
        ).drop("splitcol").dropDuplicates("article_id").distinct()
        val article_detail_info_out_path = "/region04/27367/app/develop/zs/browser_push_12"
        value_row_df.coalesce(1).write.format("json").mode("overwrite").save(article_detail_info_out_path)

        // udf
        val getidUDF = udf{click_info:String => click_info.split(",").map(_.split(":")(0)).toList}
        val dt="20201226"


        //click info
        val click_info_path = "/region04/27367/warehouse/zp_push_db/da_feature_browser_user_click_article_info/dt=%s".format(dt)
        val origin_click_df = spark.read.text(click_info_path)
        val user_click_infos_df = origin_click_df.withColumn("splitcol", split(col("value"), "\u0001")).select(
            col("splitcol").getItem(0).as("user_id"),
            get_json_object(col("splitcol").getItem(1), "$.user_click_feed_push_news_id_7day").alias("click_in_7day"),
            get_json_object(col("splitcol").getItem(1), "$.user_click_feed_push_news_id_30day").alias("click_in_30day")
        ).filter("click_in_7day is not null").withColumn("click_in_7day_array", getidUDF(col("click_in_7day"))).withColumn("click_in_30day_array", getidUDF(col("click_in_30day"))).withColumn("if_click", lit(1))
        val user_click_df = user_click_infos_df.selectExpr("user_id", "explode(click_in_7day_array) as article_id", "if_click").cache()


        // unclick info
        val unclick_info_path = "/region04/27367/warehouse/zp_push_db/da_feed_push_label_v2/dt=%s".format(dt)
        val origin_unclick_df = spark.read.text(unclick_info_path)
        val user_unclick_infos_df = origin_unclick_df.withColumn("splitcol", split(col("value"), "\u0001")).select(
            col("splitcol").getItem(0).as("if_click"),
            col("splitcol").getItem(1).as("user_id"),
            col("splitcol").getItem(2).as("article_id")
        ).drop("splitcol")


        // take user sample
        val user_df = user_click_df.select("user_id").dropDuplicates("user_id").join(user_unclick_infos_df.select("user_id").dropDuplicates("user_id"), Seq("user_id"),"left").select("user_id").filter("user_id is not null").sample(false,0.01).cache()

        // output
//        val user_click_article_detail_df = user_df.join(user_click_df, Seq("user_id"),"left").join(value_row_df, Seq("article_id"), "left").select("user_id", "article_id", "title", "content", "if_click").filter("length(content) > 20")
        val user_click_article_detail_df = user_df.join(user_click_df, Seq("user_id"),"left").join(value_row_df, Seq("article_id"), "left").select("user_id", "article_id", "if_click")
//        val user_unclick_article_detail_df = user_df.join(user_unclick_infos_df, Seq("user_id"),"left").join(value_row_df, Seq("article_id"), "left").select("user_id", "article_id", "title", "content", "if_click").filter("length(content) > 20").filter("if_click != 1")
        val user_unclick_article_detail_df = user_df.join(user_unclick_infos_df, Seq("user_id"),"left").join(value_row_df, Seq("article_id"), "left").select("user_id", "article_id", "if_click").filter("if_click != 1")

        val click_output_path = "/region04/27367/app/develop/zs/user_click_push_detail/click/dt=%s".format(dt)
        val unclick_output_path = "/region04/27367/app/develop/zs/user_click_push_detail/unclick/dt=%s".format(dt)

        user_click_article_detail_df.coalesce(10).write.format("json").mode("overwrite").save(click_output_path)
        user_unclick_article_detail_df.coalesce(10).write.format("json").mode("overwrite").save(unclick_output_path)





    }
}
