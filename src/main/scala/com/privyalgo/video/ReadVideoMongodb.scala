package com.privyalgo.video

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.privyalgo.mongodb.DBConfig
import org.bson.types.Decimal128


/**
  * Created by Joshua on 2018-11-06
  */

object ReadVideoMongodb {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ReadMongoSparkConnector")
      .getOrCreate()

    val variables = DBConfig.parseArgs(args)
    val date = variables.getOrElse("date", DBConfig.today)

    val inputUri = variables.getOrElse("operate_res_url", DBConfig.operateResUrl)
    val savePath = variables.getOrElse("operate_res_savepath", DBConfig.operateResSavePath)
    val outputPath = savePath + "/dt=%s".format(date)

    // 从mongodb读取完成标注数据 Map("mergeSchema" -> "true")
    val df = spark.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> inputUri,
        "spark.mongodb.input.readPreference.name" -> "secondary",
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"))
      .load()

//    val business_df = df.filter("business_type = 1")
//    val num = business_df.count()
//    business_df.write.mode(SaveMode.Overwrite).save(outputPath+ "/business_type=%s".format(1))
//    println("\nSuccessfully write %s data to HDFS: %s".format(num, outputPath))

    //*******//
    //视频分类//
    //*******//

    def all_video(df: DataFrame): Unit={

      val all_df = {
        df.filter("inner_type = 'video'")
          .selectExpr("id","inner_type","country","lang","text","article_title","utime",
            "ctime", "ptime", "resource_type","business_type","source_url")
      }
      val all_count = all_df.count()
      all_df.write.mode(SaveMode.Overwrite).save(outputPath + "/all_video")
      println("\nSuccessfully write %s data to HDFS: %s".format(all_count, outputPath))

    }

//    all_video(spark)


    def trial_video(df:DataFrame): Unit ={
      val b_video_df = {
        df.filter("inner_type = 'video'")
          .filter("business_type = 0")
          .filter("trial is not null")
          .filter("tags is not null")
          .selectExpr("id","inner_type","country","lang","text","article_title","utime",
            "ctime", "ptime", "resource_type","business_type","source_url", "tags", "trial.category as category",
            "trial.sub_category as sub_category", "trial.third_category as third_category",
            "trial.timeliness as timeliness","trial.review as review","trial.language_type as language_type",
            "trial.language as language","trial.is_valid as is_valid","trial.op_id as op_id",
            "trial.op_time as op_time",
            "review.category as r_category",
            "review.sub_category as r_sub_category", "review.third_category as r_third_category",
            "review.timeliness as r_timeliness","review.review as r_review","review.language_type as r_language_type",
            "review.language as r_language","review.is_valid as r_is_valid","review.op_id as r_op_id",
            "review.op_time as r_op_time", "review.is_agree as r_is_agree"
          )
      }
      val num1 = b_video_df.count()
      b_video_df.write.mode(SaveMode.Overwrite).save(outputPath + "/business_type=0")
      println("\nSuccessfully write %s data to HDFS: %s".format(num1, outputPath))

      val g_video_df = {
        df.filter("inner_type = 'video'")
          .filter("business_type = 1")
          .filter("trial is not null")
          .filter("tags is not null")
          .selectExpr("id","inner_type","country","lang","text","article_title","utime",
            "ctime", "ptime", "resource_type","business_type","source_url", "tags","trial.category as category",
            "trial.sub_category as sub_category", "trial.third_category as third_category",
            "trial.timeliness as timeliness","trial.review as review","trial.language_type as language_type",
            "trial.language as language","trial.is_valid as is_valid","trial.op_id as op_id",
            "trial.op_time as op_time",
            "review.category as r_category",
            "review.sub_category as r_sub_category", "review.third_category as r_third_category",
            "review.timeliness as r_timeliness","review.review as r_review","review.language_type as r_language_type",
            "review.language as r_language","review.is_valid as r_is_valid","review.op_id as r_op_id",
            "review.op_time as r_op_time", "review.is_agree as r_is_agree"
          )

      }
      val num2 = g_video_df.count()
      g_video_df.write.mode(SaveMode.Overwrite).save(outputPath + "/business_type=1")
      println("\nSuccessfully write %s data to HDFS: %s".format(num2, outputPath))

    }

    trial_video(df)
    //all
//    val df_list = List("article_doc_id", "article_id", "choose_keywords",
//                  "manual_keywords", "is_right", "one_level", "two_level", "three_level",
//                  "op_id", "op_name", "op_time", "server_time")
    //test
//    val df_list = List("article_doc_id", "article_id", "choose_keywords",
//                "is_right", "one_level", "two_level", "op_time", "server_time")

//    val originDf = df.select(df_list.map(col): _*)
//    val num = originDf.count()

//    originDf.repartition(1).write.mode(SaveMode.Overwrite).parquet(outputPath)
//    val out_df = df.filter("resource_type = 20002 or resource_type = 11").filter("ctime < 1554195600")
//    val out_df = df.filter("resource_type in (20002,11,20101,20104,20105,20106,20107,20108,20109,20110,20112)")//.filter("ctime < 1554195600")
//    out_df.show()
//    val num = out_df.count()
//    println(num)
//    out_df.write.mode(SaveMode.Overwrite).save(outputPath)


//    val out_df1 = df.filter("resource_type = 20002").filter("ctime < 1554195600")
//    val num1 = out_df1.count()
//    out_df1.write.mode(SaveMode.Overwrite).save(outputPath+ "/rt=%s".format(20002))
//    println("\nSuccessfully write %s data to HDFS: %s".format(num1, outputPath))

    // youtube 6（11）id修改过
//    val out_df2 = df.filter("resource_type = 6").filter("ctime < 1554195600")
//    val num2 = out_df2.count()
//    out_df2.write.mode(SaveMode.Overwrite).save(outputPath+ "/rt=%s".format(6))
//    println("\nSuccessfully write %s data to HDFS: %s".format(num2, outputPath))

//    val out_df3 = df.filter("resource_type = 20101").filter("ctime < 1554195600")
//    val num3 = out_df3.count()
//    out_df3.write.mode(SaveMode.Overwrite).save(outputPath+ "/rt=%s".format(20101))
//    println("\nSuccessfully write %s data to HDFS: %s".format(num3, outputPath))
//
//    val out_df4 = df.filter("resource_type = 20104").filter("ctime < 1554195600")
//    val num4 = out_df4.count()
//    out_df4.write.mode(SaveMode.Overwrite).save(outputPath+ "/rt=%s".format(20104))
//    println("\nSuccessfully write %s data to HDFS: %s".format(num4, outputPath))
//
//    val out_df5 = df.filter("resource_type = 20105").filter("ctime < 1554195600")
//    val num5 = out_df5.count()
//    out_df5.write.mode(SaveMode.Overwrite).save(outputPath+ "/rt=%s".format(20105))
//    println("\nSuccessfully write %s data to HDFS: %s".format(num5, outputPath))
//
//    val out_df6 = df.filter("resource_type = 20106").filter("ctime < 1554195600")
//    val num6 = out_df6.count()
//    out_df6.write.mode(SaveMode.Overwrite).save(outputPath+ "/rt=%s".format(20106))
//    println("\nSuccessfully write %s data to HDFS: %s".format(num6, outputPath))
//
//    val out_df7 = df.filter("resource_type = 20107").filter("ctime < 1554195600")
//    val num7 = out_df7.count()
//    out_df7.write.mode(SaveMode.Overwrite).save(outputPath+ "/rt=%s".format(20107))
//    println("\nSuccessfully write %s data to HDFS: %s".format(num7, outputPath))
//
//    val out_df8 = df.filter("resource_type = 20108").filter("ctime < 1554195600")
//    val num8 = out_df8.count()
//    out_df8.write.mode(SaveMode.Overwrite).save(outputPath+ "/rt=%s".format(20108))
//    println("\nSuccessfully write %s data to HDFS: %s".format(num8, outputPath))
//
//    val out_df9 = df.filter("resource_type = 20109").filter("ctime < 1554195600")
//    val num9 = out_df9.count()
//    out_df9.write.mode(SaveMode.Overwrite).save(outputPath+ "/rt=%s".format(20109))
//    println("\nSuccessfully write %s data to HDFS: %s".format(num9, outputPath))
//
//    val out_df10 = df.filter("resource_type = 20110").filter("ctime < 1554195600")
//    val num10 = out_df10.count()
//    out_df10.write.mode(SaveMode.Overwrite).save(outputPath+ "/rt=%s".format(20110))
//    println("\nSuccessfully write %s data to HDFS: %s".format(num10, outputPath))
//
//    val out_df11 = df.filter("resource_type = 20112").filter("ctime < 1554195600")
//    val num11 = out_df11.count()
//    out_df11.write.mode(SaveMode.Overwrite).save(outputPath+ "/rt=%s".format(20112))
//    println("\nSuccessfully write %s data to HDFS: %s".format(num11, outputPath))

    spark.stop()
  }
}
