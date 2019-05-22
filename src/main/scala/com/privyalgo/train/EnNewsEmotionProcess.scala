package com.privyalgo.train

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



/**
  * Created by Joshua on 19-5-22
  */
object EnNewsEmotionProcess {

  def main(args: Array[String]): Unit = {
    val appName = "En-News-Emotion-Regional-Timelines-Taste-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext


    //**********************************//
    //印度英语新闻情感、地域、浏览口味、时间处理//
    //**********************************//

    val marked_path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/label_result/batch_id=*"
    val df = spark.read.json(marked_path)
    val filtered_df = {
      df.filter("batch_id in (2019041902,2019041901,2019041702,2019041701,2019041505,2019041502,2019041501, 2019040401)")
        .filter("emotion is not null")
        .filter("region is not null")
        .filter("taste is not null")
        .filter("timeliness is not null")
        .selectExpr("resource_id as id","title","text","emotion","region","taste","timeliness")
    }

    filtered_df.repartition(1).write.format("json").mode("overwrite").save("news_content/emotion_region_taste_timeliness")


//    val emotion_df = {
//      df.filter("batch_id in (2019041902,2019041901,2019041702,2019041701,2019041505,2019041502,2019041501, 2019040401)")
//        .filter("emotion is not null")
//        .selectExpr("resource_id as id","title","text","emotion","region","taste","timeliness")
//    }
//    val region_df = {
//      df.filter("batch_id in (2019041902,2019041901,2019041702,2019041701,2019041505,2019041502,2019041501, 2019040401)")
//        .filter("region is not null")
//        .selectExpr("resource_id as id","title","text","emotion","region","taste","timeliness")
//    }
//    val taste_df = {
//      df.filter("batch_id in (2019041902,2019041901,2019041702,2019041701,2019041505,2019041502,2019041501, 2019040401)")
//        .filter("taste is not null")
//        .selectExpr("resource_id as id","title","text","emotion","region","taste","timeliness")
//    }
//    val timeliness_df = {
//      df.filter("batch_id in (2019041902,2019041901,2019041702,2019041701,2019041505,2019041502,2019041501, 2019040401)")
//        .filter("timeliness is not null")
//        .selectExpr("resource_id as id","title","text","emotion","region","taste","timeliness")
//    }


  }
}
