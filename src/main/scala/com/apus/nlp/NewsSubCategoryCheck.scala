package com.apus.nlp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, udf}
import org.apache.spark.sql.types.StringType

/**
  * Created by Joshua on 2019-01-16
  */
object NewsSubCategoryCheck {
  def main(args: Array[String]): Unit = {

    //------------------------------------1 财经数据分类查看 -----------------------------------------
    //
    def business_check(spark: SparkSession,
                       newsPath: String,
                       dt: String = "2019-01-15") = {
      val newsPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
          .selectExpr("resource_id as article_id", "url")
      }

      val train = spark.read.json("news_content/subcategory_check/business_v1_train")
      //      val train_df = train.join(ori_df, Seq("article_id"))
      val train_df = spark.read.json("news_content/subcategory_check/business_v1_train")
      val out1 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'market' and predict_two_level = 'industry economic'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out2 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'investment' and predict_two_level = 'industry economic'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out3 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'company' and predict_two_level = 'industry economic'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out4 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'others' and predict_two_level = 'industry economic'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out5 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'others' and predict_two_level = 'investment'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out6 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'industry economic' and predict_two_level = 'company'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out7 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'industry economic' and predict_two_level = 'company'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out8 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'others' and predict_two_level = 'market'").limit(10)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out = out1.union(out2).union(out3).union(out4).union(out5).union(out6).union(out7).union(out8)
      out.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/tmp/out3")

    }


    //------------------------------------2 世界数据查看国家 -----------------------------------------
    //
    // US,China,Pakistan,Korea,North,India,Russia,UK,Syria,Iran,Saudi,Israel,UN

    def world_check_country(spark: SparkSession) = {
      val path = "/user/caifuli/news/all_data/international_classified"

      val markDF = spark.read.json(path)

      val mark_id = markDF.select(col("news_id").cast(StringType), col("resource_id"))
      val markall = {
        val seqUDF = udf((t: String) => Seq.empty[String])
        val cleanUDF = udf((level: String) => if (level == null || level.replace(" ", "") == "") "others" else level.trim().toLowerCase) // 增加清洗分类
        markDF.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
          .selectExpr("article_id", "title", "url as article_url", "top_category", "sub_category", "third_category", "length(content) as article_len", "length(html) as html_len")
      }.dropDuplicates("article_id", "title")
      val title_df = {
        markall.rdd.flatMap {
          row =>
            val title = row.getAs[String]("title").split(" ")
            title
        }.map(word => (word, 1)).filter(_._1.length > 1).filter(_._1.slice(0, 1).map(_.toByte).toList(0) <= 90).reduceByKey(_ + _).sortBy(_._2, ascending = false)
      }
      title_df.coalesce(1).saveAsTextFile("news_content/tmp/country_count")
    }
  }