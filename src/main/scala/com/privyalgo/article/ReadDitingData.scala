package com.privyalgo.article

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object ReadDitingData {

    def main(args: Array[String]): Unit = {
        val appName = "Read-DitingResult"
        val spark = SparkSession.builder()
          .master("local")
          .appName(appName)
          .getOrCreate()
        val sc = spark.sparkContext

        val basic_table_title_path = "/region04/22135/warehouse/feedscontent/ods_feedscontent_article_core_hi/day=2020-0[1-4]-*/hour=*"
        val basic_table_content_path = "/region04/22135/warehouse/feedscontent/ods_feedscontent_article_core_es_hi/day=2020-0[1-4]-*/hour=*"
        val title_df = spark.read.text(basic_table_title_path)
        val content_df = spark.read.text(basic_table_content_path)

        val title_row_df = title_df.withColumn("splitcol", split(col("value"), "\t")).select(
            col("splitcol").getItem(0).as("article_id"),
            col("splitcol").getItem(3).as("source"),
            col("splitcol").getItem(4).as("category"),
            col("splitcol").getItem(5).as("channel"),
            col("splitcol").getItem(6).as("title"),
            col("splitcol").getItem(7).as("originalurl"),
            col("splitcol").getItem(14).as("articletype"),
            col("splitcol").getItem(15).as("tags")
        ).drop("splitcol").filter("length(title) > 0").dropDuplicates("article_id").distinct().cache


        val content_row_df = content_df.withColumn("splitcol", split(col("value"), "\t")).select(
            col("splitcol").getItem(0).as("article_id"),
            col("splitcol").getItem(1).as("content"),
//            col("splitcol").getItem(2).as("createtime"),
//            col("splitcol").getItem(3).as("updatetime"),
            col("splitcol").getItem(4).as("updatedbtime")
        ).drop("splitcol").filter("length(content) > 0").dropDuplicates("article_id").distinct().cache


//        val diting_result_path = "/region04/22135/warehouse/feedscontent/ods_feedscontent_article_core_analyze_hi/day=2020-04-*/hour=*"
//        val output_path = "/region04/27367/app/develop/zs/article_diting/dt=2020-04"
        val diting_result_path = "/region04/22135/warehouse/feedscontent/ods_feedscontent_article_core_analyze_hi/day=2020-0[1-3]-*/hour=*"
        val output_path = "/region04/27367/app/develop/zs/article_diting_1-3"

        val diting_df = spark.read.text(diting_result_path)
        val diting_row_df = diting_df.withColumn("splitcol", split(col("value"), "\t")).select(
            col("splitcol").getItem(0).as("article_id"),
            col("splitcol").getItem(1).as("createtime"),
            col("splitcol").getItem(2).as("updatetime"),
            col("splitcol").getItem(3).as("tagvec"),
            col("splitcol").getItem(4).as("topcategory"),
            col("splitcol").getItem(5).as("subcategory"),
            col("splitcol").getItem(6).as("quality"),
            col("splitcol").getItem(7).as("thirdcategory"),
            col("splitcol").getItem(8).as("tagscoredata"),
            col("splitcol").getItem(9).as("reviseresult"),
            col("splitcol").getItem(10).as("ditingstatus")
        ).drop("splitcol").filter("length(tagscoredata) > 0")

        val diting_title_df = diting_row_df.join(title_row_df, Seq("article_id"), "left").select("article_id", "title", "source", "channel",
              "originalurl", "tagscoredata", "reviseresult",
              "ditingstatus", "category", "topcategory", "subcategory", "thirdcategory",
              "articletype", "tags", "tagvec", "quality","createtime", "updatetime")
        val diting_result_df =   diting_title_df.join(content_row_df, Seq("article_id"), "left").dropDuplicates("article_id").distinct()

        diting_result_df.coalesce(20).write.format("json").mode("overwrite").save(output_path)



    }
}
