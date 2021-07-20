package com.privyalgo.article

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, _}


object ReadVideoOcrData {

    def main(args: Array[String]): Unit = {
        val appName = "Read-Video-Ocr-Data"
        val spark = SparkSession.builder()
          .master("local")
          .appName(appName)
          .getOrCreate()
        val sc = spark.sparkContext

        // 视频信息
        val video_table_path = "/region04/27367/warehouse/zp_push_db/da_video_resource_day_full_dose/day=20210409"
        val video_df = spark.read.text(video_table_path)
        val video_row_df = video_df.withColumn("splitcol", split(col("value"), "\u0001")).select(
            col("splitcol").getItem(0).as("video_id"),
            col("splitcol").getItem(2).as("article_type"),
            col("splitcol").getItem(11).as("images"),
            col("splitcol").getItem(17).as("music_name"),
            col("splitcol").getItem(22).as("source"),
            col("splitcol").getItem(25).as("tags"),
            col("splitcol").getItem(26).as("title"),
            col("splitcol").getItem(28).as("user_description"),
            col("splitcol").getItem(30).as("user_nick_name"),
            col("splitcol").getItem(34).as("video_url"),
            col("splitcol").getItem(57).as("diting_result_category_list")
        ).drop("splitcol").filter("article_type in (2,3,4)").filter("diting_result_category_list is not null and diting_result_category_list != 'null'").dropDuplicates("video_id").distinct()

        // ocr信息
        // 读取2020年10，11，12月视频数据及2021年1-4月数据
        val ocr_table_path1 = "/region04/27367/app/product/video_content/result/all_result/dt=20201*/hour=*"
        val ocr_table_path2 = "/region04/27367/app/product/video_content/result/all_result/dt=20210*/hour=*"
        val ocr_df1 = spark.read.text(ocr_table_path1)
        val ocr_df2 = spark.read.text(ocr_table_path2)
        val ocr_row_df1 = ocr_df1.withColumn("splitcol", split(col("value"), "\u0001")).select(
            col("splitcol").getItem(27).as("video_id"),
            col("splitcol").getItem(6).as("video_ocr"),
//            col("splitcol").getItem(11).as("category"),
//            col("splitcol").getItem(12).as("subcategory"),
//            col("splitcol").getItem(18).as("origin_info"),
//            col("splitcol").getItem(21).as("video_category"),
            col("splitcol").getItem(25).as("ocr_filter_result")
        ).drop("splitcol").dropDuplicates("video_id").distinct()
        val ocr_row_df2 = ocr_df2.withColumn("splitcol", split(col("value"), "\u0001")).select(
            col("splitcol").getItem(27).as("video_id"),
            col("splitcol").getItem(6).as("video_ocr"),
//            col("splitcol").getItem(11).as("category"),
//            col("splitcol").getItem(12).as("subcategory"),
//            col("splitcol").getItem(18).as("origin_info"),
//            col("splitcol").getItem(21).as("video_category"),
            col("splitcol").getItem(25).as("ocr_filter_result")
        ).drop("splitcol").dropDuplicates("video_id").distinct()

        val ocr_row_df = ocr_row_df1.union(ocr_row_df2).distinct()
        ocr_row_df.write.format("json").mode("overwrite").save("/region04/27367/app/develop/zs/video_ocr_corpus")
        val video_info_df = video_row_df.join(ocr_row_df, Seq("video_id"), "left").distinct()
//        val output_path = "/region04/27367/app/develop/zs/video_ocr_9"
        val output_path = "/region04/27367/app/develop/zs/video_category_corpus"
        video_info_df.coalesce(5).write.format("json").mode("overwrite").save(output_path)


        // 按照月份写ocr数据
        val month = "202104"
        val ocr_table_path = "/region04/27367/app/product/video_content/result/all_result/dt=%s*/hour=*".format(month)
        val ocr_df = spark.read.text(ocr_table_path)
        val ocr_row_df = ocr_df.withColumn("splitcol", split(col("value"), "\u0001")).select(
            col("splitcol").getItem(27).as("video_id"),
            col("splitcol").getItem(6).as("video_ocr"),
            col("splitcol").getItem(0).as("cover_ocr"),
            col("splitcol").getItem(5).as("video_ocr_tmp"),
            col("splitcol").getItem(25).as("ocr_filter_result")
        ).drop("splitcol").dropDuplicates("video_id").distinct()
        val save_path = "/region04/27367/app/develop/zs/video_ocr_corpus/dt=%s".format(month)
        ocr_row_df.coalesce(10).write.format("json").mode("overwrite").save(save_path)

        val ocr = spark.read.json(save_path)
        val out_ocr = ocr.selectExpr("video_id as aid", "video_ocr", "ocr_filter_result")
        val out_save_path = "/region04/27367/app/develop/zs/ocr_corpus/dt=%s".format(month)
        out_ocr.coalesce(1).write.format("json").mode("overwrite").save(out_save_path)

        // 按照月份写美女标签数据
        val month = "202101"
        val ocr_table_path = "/region04/27367/app/product/video_content/result/all_result/dt=%s*/hour=*".format(month)
        val all_df = spark.read.text(ocr_table_path)
        val beauty_row_df = all_df.withColumn("splitcol", split(col("value"), "\u0001")).select(
            col("splitcol").getItem(27).as("video_id"),
            col("splitcol").getItem(29).as("video_beauty")
        ).drop("splitcol").filter("video_beauty != '[]'").distinct()
        val save_path = "/region04/27367/app/develop/zs/video_beauty_corpus/dt=%s".format(month)
        beauty_row_df.coalesce(10).write.format("json").mode("overwrite").save(save_path)

        val path = "/region04/27367/app/develop/zs/video_beauty_corpus/dt=*"
        val df = spark.read.json(path)
        df.filter("video_beauty != '[{\"error_code\":0}]'").coalesce(1).write.format("json").mode("overwrite").save("/region04/27367/app/develop/zs/video_beauty_corpus_not_null")
    }
}
