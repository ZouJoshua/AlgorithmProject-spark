package com.apus.mark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.jsoup.Jsoup

/**
  * Created by Joshua on 2019-02-21
  */
object NewsSubCategoryMarchTrainProcess {
  def main(args: Array[String]): Unit = {

    val appName = "News-SubCategory-March-MarkedCorpus-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext


    val dt = "2019-02-21"
    val newsPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"

    //------------------------------------1 更新国内二级分类 -----------------------------------------
    // 1.去除three_level 为null
    // 2.抽取three_level 为crime、law
    // 3.抽取two_level 为politics、education
    // 4.其他放为others(量不足)

    def update_national_process(spark: SparkSession,
                                   newsPath: String="/user/hive/warehouse/apus_dw.db/dw_news_data_hour",
                                   dt: String="2019-02-21") = {


    }

    def update_tech_process(spark: SparkSession,
                                newsPath: String="/user/hive/warehouse/apus_dw.db/dw_news_data_hour",
                                dt: String="2019-02-21") = {

      val tech_check_path = "news_content/sub_classification/tech/tech_check"

      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df = spark.read.parquet(path)
      val tech_df = df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time").filter("one_level = 'tech'")



      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
          .selectExpr("resource_id as article_id", "html", "title", "url")
          .withColumn("content", getcontentUDF(col("html")))
          .drop("html")
      }
      val df1 = {
        val cleanUDF = udf{(word: String) => word.toLowerCase().replace("mobile phone","mobile-phone").replace("apps", "app").replace(" ", "").replace("mobile-phone", "mobile phone")}
        spark.read.json(tech_check_path)
          .filter("top_category in ('tech', 'Tech','tech')")
          .withColumnRenamed("news_id", "article_id")
          .withColumn("one_level", cleanUDF(col("top_category")))
          .withColumn("two_level", cleanUDF(col("sub_category")))
          .withColumn("three_level", lit("others"))
          .select("article_id","one_level", "two_level", "three_level").dropDuplicates("article_id")
      }

      val result = {
        val others = Seq("sci-tech", "internet", "reviews", "tablet", "software", "others")
        val replaceUDF = udf{(word:String) => if(others.contains(word)) "others" else word}
        df1.join(ori_df,Seq("article_id"))
          .filter("two_level in ('mobile phone','app', 'gadget','computer','sci-tech', 'internet', 'reviews', 'tablet', 'software', 'others')")
          .withColumn("two_level_new", replaceUDF(col("two_level")))
          .drop("two_level")
          .withColumnRenamed("two_level_new", "two_level")
      }
      println(">>>>>>>>>>正在写入数据")
      result.write.mode("overwrite").save("news_content/sub_classification/tmp/tech_all")
      val redf = spark.read.parquet("news_content/sub_classification/tmp/tech_all")
      redf.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/tech/tech_all")
      println(">>>>>>>>>>写入数据完成")


    }

    def update_business_process(spark: SparkSession,
                                newsPath: String="/user/hive/warehouse/apus_dw.db/dw_news_data_hour",
                                dt: String="2019-02-21") = {


    }

    def update_entertainment_process(spark: SparkSession,
                                newsPath: String="/user/hive/warehouse/apus_dw.db/dw_news_data_hour",
                                dt: String="2019-02-21") = {


    }



  }
}
