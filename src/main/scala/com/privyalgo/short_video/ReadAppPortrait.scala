package com.privyalgo.short_video

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, _}


/**
 * Created by Joshua on 2021/7/14
 */
object ReadAppPortrait {
    def main(args: Array[String]): Unit = {
        val appName = "Read-DYApp-UserPortrait-Data"
        val spark = SparkSession.builder()
          .master("local")
          .appName(appName)
          .getOrCreate()
        val sc = spark.sparkContext
        val user_portrait_path = "/region02/21689/warehouse/ai_platform_dmp_data/da_label_data_for_sv_test_qm_v1"
        val text_row = spark.read.text(user_portrait_path)
        val df_row = text_row.withColumn("splitcol", split(col("value"), "\u0001")).select(
            col("splitcol").getItem(0).as("label_id"),
            col("splitcol").getItem(1).as("value")
            ).drop("splitcol").distinct()
        val user_df = df_row.select(get_json_object(col("value"),"$.6_6_21_1").alias("keywords_day7_top50_str"),
            get_json_object(col("value"),"$.6_6_21_2").alias("keywords_day90_top50_str"),
            get_json_object(col("value"),"$.6_6_21_3").alias("topic_day7_top20_str"),
            get_json_object(col("value"),"$.6_6_21_4").alias("blogger_day14_top20_str")
        )
        val getMapUDF = udf{user_info:String =>
            val out_map = user_info.split(",").map(x => (x.split(":").head, x.split(":").last)).toMap
            out_map
        }
        val getListUDF = udf{map_info:Map[String, String] => map_info.keys.toList}

        //-------------------------------------------
        //  单独输出统计排序后的用户画像关键词
        val all_keywords_day7 = user_df.filter("keywords_day7_top50_str is not null").withColumn("keywords_day7_top50_map", getMapUDF(col("keywords_day7_top50_str"))).withColumn("keywords_day7_top50", getListUDF(col("keywords_day7_top50_map"))).selectExpr("explode(keywords_day7_top50) as keywords_day7").groupBy("keywords_day7").agg(count("keywords_day7").as("count")).sort(desc("count"))
        val all_keywords_day90 = user_df.filter("keywords_day90_top50_str is not null").withColumn("keywords_day90_top50_map", getMapUDF(col("keywords_day90_top50_str"))).withColumn("keywords_day90_top50", getListUDF(col("keywords_day90_top50_map"))).selectExpr("explode(keywords_day90_top50) as keywords_day90").groupBy("keywords_day90").agg(count("keywords_day90").as("count")).sort(desc("count"))
        val all_topic_day7 = user_df.filter("topic_day7_top20_str is not null").withColumn("topic_day7_top20_map", getMapUDF(col("topic_day7_top20_str"))).withColumn("topic_day7_top20", getListUDF(col("topic_day7_top20_map"))).selectExpr("explode(topic_day7_top20) as topic_day7").groupBy("topic_day7").agg(count("topic_day7").as("count")).sort(desc("count"))
        val all_blogger_day7 = user_df.filter("blogger_day14_top20_str is not null").withColumn("blogger_day14_top20_map", getMapUDF(col("blogger_day14_top20_str"))).withColumn("blogger_day14_top20", getListUDF(col("blogger_day14_top20_map"))).selectExpr("explode(blogger_day14_top20) as blogger_day14").groupBy("blogger_day14").agg(count("blogger_day14").as("count")).sort(desc("count"))

        val all_keywords_day7_out = "/region04/27367/app/develop/zs/aie_user_portrait/all_keywords_day7"
        val all_keywords_day90_out = "/region04/27367/app/develop/zs/aie_user_portrait/all_keywords_day90"
        val all_topic_day7_out = "/region04/27367/app/develop/zs/aie_user_portrait/all_topic_day7"
        val all_blogger_day7_out = "/region04/27367/app/develop/zs/aie_user_portrait/all_blogger_day7"

        all_keywords_day7.coalesce(10).write.format("json").mode("overwrite").save(all_keywords_day7_out)
        all_keywords_day90.coalesce(10).write.format("json").mode("overwrite").save(all_keywords_day90_out)
        all_topic_day7.coalesce(10).write.format("json").mode("overwrite").save(all_topic_day7_out)
        all_blogger_day7.coalesce(10).write.format("json").mode("overwrite").save(all_blogger_day7_out)

        //-------------------------------------------
        // 输出关键词偏好top50-7天、关键词偏好top50-90天、话题偏好top20-7天交叉关键词排序后的用户画像
        val all_map_info = {
            user_df.filter("keywords_day7_top50_str is not null")
              .filter("keywords_day90_top50_str is not null")
              .filter("topic_day7_top20_str is not null")
              .withColumn("keywords_day7_top50_map", getMapUDF(col("keywords_day7_top50_str"))).withColumn("keywords_day90_top50_map", getMapUDF(col("keywords_day90_top50_str"))).withColumn("topic_day7_top20_map", getMapUDF(col("topic_day7_top20_str"))).drop("keywords_day7_top50_str","keywords_day90_top50_str", "topic_day7_top20_str", "blogger_day14_top20_str")
        }
        val all_list_info = {
            all_map_info.withColumn("keywords_day7_top50", getListUDF(col("keywords_day7_top50_map")))
              .withColumn("keywords_day90_top50", getListUDF(col("keywords_day90_top50_map")))
              .withColumn("topic_day7_top20", getListUDF(col("topic_day7_top20_map")))
        }
        val getIntersectUDF = udf{(array1: Seq[String], array2:Seq[String], array3:Seq[String]) =>
            val result1 = array1.toSet & array2.toSet
            val result = result1 & array3.toSet
              result.toSeq
        }

        val all_common_keywords = {
            all_list_info
              .filter("keywords_day7_top50 is not null")
              .filter("keywords_day90_top50 is not null")
              .filter("topic_day7_top20 is not null")
              .withColumn("common_keywords_list", getIntersectUDF(col("keywords_day7_top50"), col("keywords_day90_top50"), col("topic_day7_top20")))
              .filter("common_keywords_list is not null").selectExpr("explode(common_keywords_list) as common_keywords")
              .groupBy("common_keywords").agg(count("common_keywords").as("count")).sort(desc("count"))
        }
        val all_common_keywords_out = "/region04/27367/app/develop/zs/aie_user_portrait/all_common_keywords"
        all_common_keywords.coalesce(10).write.format("json").mode("overwrite").save(all_common_keywords_out)

        //-------------------------------------------
        // 输出关键词偏好top50-7天、关键词偏好top50-90天、话题偏好top20-7天所有关键词排序后的用户画像
        val getCollectionUDF = udf{(array1: Seq[String], array2:Seq[String], array3:Seq[String]) =>
            val result = array1++array2++array3
            result.toList
        }
        val all_keywords = {
            all_list_info
              .withColumn("all_keywords_list", getCollectionUDF(col("keywords_day7_top50"), col("keywords_day90_top50"), col("topic_day7_top20")))
              .selectExpr("explode(all_keywords_list) as keywords_collection").groupBy("all_keywords").agg(count("all_keywords").as("count")).sort(desc("count"))
        }
        val all_keywords_out = "/region04/27367/app/develop/zs/aie_user_portrait/all_keywords_collection"
        all_keywords.coalesce(10).write.format("json").mode("overwrite").save(all_keywords_out)

        //-------------------------------------------
        // 使用ownthink过滤关键词
        val all_keywords_collection_path = "/region04/27367/app/develop/zs/aie_user_portrait/all_keywords_collection"
        val all_keywords_collection = spark.read.format("json").load(all_keywords_collection_path).withColumnRenamed("all_keywords", "entity_word")
        val ownthink_path = "/region04/27367/app/develop/zs/aie_user_portrait/ownthink"
        val ownthink = {
            spark.read.format("csv").option("header", "true").load(ownthink_path)
              .withColumnRenamed("实体", "entity_word")
              .withColumnRenamed("属性", "attributes")
              .withColumnRenamed("值", "value")
        }

        val ownthink_entity_words = ownthink.select("entity_word").distinct.withColumn("if_entity", lit(true))
        val entity_word_in_ownthink = all_keywords_collection.join(ownthink_entity_words, Seq("entity_word"), "left").filter("if_entity is not null")

        val output_path = "/region04/27367/app/develop/zs/aie_user_portrait/entity_words_in_ownthink"
        entity_word_in_ownthink.coalesce(1).write.format("json").mode("overwrite").save(output_path)

        //-------------------------------------------
        // 用户兴趣与ownthink交集添加属性
        val ownthink_path = "/region04/27367/app/develop/zs/aie_user_portrait/ownthink"
        val ownthink = {
            spark.read.format("csv").option("header", "true").load(ownthink_path)
              .withColumnRenamed("实体", "entity_word")
              .withColumnRenamed("属性", "attributes")
              .withColumnRenamed("值", "value")
        }
        val entity_words_path = "/region04/27367/app/develop/zs/aie_user_portrait/entity_words_in_ownthink"
        val entity_words = spark.read.json(entity_words_path)
        val entity_words_in_ownthink_with_attributes = ownthink.join(entity_words, Seq("entity_word"), "left").filter("if_entity is not null")
        val output = "/region04/27367/app/develop/zs/aie_user_portrait/entity_words_in_ownthink_with_attributes"

        entity_words_in_ownthink_with_attributes.coalesce(1).write.format("json").mode("overwrite").save(output)

        val attributes = entity_words_in_ownthink_with_attributes.groupBy("attributes").agg(count("attributes").as("count")).sort(desc("count"))
        attributes.coalesce(1).write.format("json").mode("overwrite").save("/region04/27367/app/develop/zs/aie_user_portrait/entity_words_attributes_key")
    }
}
