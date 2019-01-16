package com.apus.nlp

import org.apache.spark.sql.SparkSession

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

      val train = spark.read.json("news_content/subcategory_check/business_test")
      val train_df = train.join(ori_df, Seq("article_id"))
      val out1 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'market' and predict_two_level = 'finance'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out2 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'investment' and predict_two_level = 'finance'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out3 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'industry economic' and predict_two_level = 'finance'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out4 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'industry economic' and predict_two_level = 'investment'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out5 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'market' and predict_two_level = 'investment'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out6 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'others' and predict_two_level = 'investment'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out7 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'industry economic' and predict_two_level = ' others'").limit(10)
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

  }
}
