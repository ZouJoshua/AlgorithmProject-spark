package com.privyalgo.short_video

import com.privyalgo.nlp.PrepProcess.cleanString
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, _}

import scala.collection.mutable



/**
 * Created by Joshua on 2021/7/23
 */
object ReadContent {
    def main(args: Array[String]): Unit = {
        val appName = "Read-Video-Full-Dose-Data"
        val spark = SparkSession.builder()
          .master("local")
          .appName(appName)
          .getOrCreate()
        val sc = spark.sparkContext
        //-------------------------------------------
        //  全量id数据
        val full_dose_path = "/region04/27367/warehouse/zp_push_db/da_video_resource_day_full_dose_filter/day=20210725"
        val full_dose_df = spark.read.text(full_dose_path).withColumn("splitcol", split(col("value"), "\u0001")).select(
            col("splitcol").getItem(1).as("video_id"),
            col("splitcol").getItem(181).as("video_core_words")
        ).drop("splitcol").distinct()
//        full_dose_df.filter("video_core_words is not null").filter("video_core_words != '[]'").filter("video_core_words != 'NULL'").filter("video_core_words != 'null'").filter(_.getAs[String]("video_core_words") != "\\N").show

        //-------------------------------------------
        //  内容数据
        val base_path = "/region04/27367/app/product/video_content/result/all_result"
        val content_path = "/region04/27367/app/product/video_content/result/all_result/dt=20210[2-7]*/hour=*"
//        val all_content_df = spark.read.text(content_path)
        val all_content_df = spark.read.option("basePath", base_path).text(content_path)
        val content_df = {
            all_content_df.withColumn("splitcol", split(col("value"), "\u0001")).select(
                col("splitcol").getItem(27).as("video_id"),
                get_json_object(col("splitcol").getItem(18),"$.title").alias("title"),
                col("splitcol").getItem(6).as("video_ocr")
            ).drop("splitcol").dropDuplicates("video_id").distinct()
        }

        val full_dose_with_content_df = full_dose_df.join(content_df, Seq("video_id"))
        val output_path = "/region04/27367/app/develop/zs/video_content_with_core_words/day=20210725"
        full_dose_with_content_df.coalesce(100).write.format("json").mode("overwrite").save(output_path)

        //-------------------------------------------
        //  核心实体词
        val core_entity_words_path = "/region04/27367/app/develop/zs/aie_user_portrait/add_core_entity_words_with_part_of_speech"
        val core_entity_words = {
            spark.read.text(core_entity_words_path).withColumn("splitcol", split(col("value"), "\t")).select(
                col("splitcol").getItem(0).as("entity_word"),
                col("splitcol").getItem(1).as("count"),
                col("splitcol").getItem(2).as("part_of_speech")
            ).drop("splitcol")
        }


//        val keywords_day7 = spark.read.json("/region04/27367/app/develop/zs/aie_user_portrait/all_keywords_day7")
        val keywords_day7 = spark.read.json("/region04/27367/app/develop/zs/aie_user_portrait/filter_keywords_day7")
        val entity_word_seq = keywords_day7.rdd.map(x => x.getAs[String]("keywords_day7")).collect().toSeq


        val entity_word_seq = core_entity_words.rdd.map(x => x.getAs[String]("entity_word")).collect().toSeq
        val entityWordSeq = sc.broadcast(entity_word_seq)

        //-------------------------------------------
        //  统计包含核心实体词items
        val content_df = spark.read.json(output_path)
//        content_df.filter(_.getAs[String]("video_core_words") != "\N")
        val art_wiki_df = content_df.rdd.map{
            r =>
                val id = r.getAs[String]("video_id").toString
                val title = r.getAs[String]("title")
                val ocr = r.getAs[String]("video_ocr")
                val text = title + ocr
                var hit_dict: List[String] = List()
                //        var hit_dict = Seq.empty[String]
                for(word <- entityWordSeq.value){
                    if(text.contains(word)){
                        hit_dict = word :: hit_dict
                    }}
                (id,hit_dict)
        }.toDF("video_id", "hit_entity_keywords")
        art_wiki_df.coalesce(1).write.format("json").mode("overwrite").save("/region04/27367/app/develop/zs/aie_user_portrait/tmp/hit_entity_keywords")
        art_wiki_df.coalesce(10).write.format("json").mode("overwrite").save("/region04/27367/app/develop/zs/aie_user_portrait/tmp/hit_keywords_day7")

        //-------------------------------------------
        //  统计命中的关键词的item的排序
        val df = spark.read.json("/region04/27367/app/develop/zs/aie_user_portrait/tmp/hit_entity_keywords")
        val out = {
            df.filter("size(hit_entity_keywords) > 0").rdd.map{
                r =>
                    val id  = r.getAs[String]("video_id")
                    val keywords = r.getAs[mutable.WrappedArray[String]]("hit_entity_keywords").toSeq.distinct
                    (id, keywords)
            }.toDF("video_id", "entity_words_set").selectExpr("explode(entity_words_set) as core_entity_words").groupBy("core_entity_words").agg(count("core_entity_words").as("count")).sort(desc("count"))
        }
        out.coalesce(1).write.format("json").mode("overwrite").save("/region04/27367/app/develop/zs/aie_user_portrait/tmp/core_entity_words")

        //-------------------------------------------
        //  核心实体词在召回item中覆盖查询

        val recall_all_path = "/region04/27367/warehouse/zp_push_db/dm_svideo_imei_content_kpi_details/day=2021-07-25"
        val recall_all_df = {
            spark.read.option("inferSchema", true).orc(recall_all_path)
              .selectExpr("content_id as video_id", "rec", "expid", "new_play_cnt").distinct()
        }

        val recall_item_core_entity_word = {
            recall_all_df.filter("expid like '%1139510101%'")
              .join(content_df, Seq("video_id"))
        }
        recall_item_core_entity_word.coalesce(100).write.format("json").mode("overwrite").save("/region04/27367/app/develop/zs/aie_user_portrait/tmp/expid_dict_user_keywords/day=20210725")

        val rec_recall_item_core_entity_word = {
            recall_item_core_entity_word.filter("rec like '%602%'")
              .filter("new_play_cnt > 0")
              .filter("video_core_words is not null")
              .filter("video_core_words != '[]'")
              .filter("video_core_words != 'NULL'")
              .filter("video_core_words != 'null'")
              .filter(_.getAs[String]("video_core_words") != "\\N")
        }
        rec_recall_item_core_entity_word.coalesce(1).write.format("json").mode("overwrite").save("/region04/27367/app/develop/zs/aie_user_portrait/tmp/expid_rec_dict_user_keywords/day=20210725")





    }
}
