package com.apus.nlp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, TextNode}

/**
  * Created by Joshua on 2019-01-07
  */
object NewsSubCategory {
  def main(args: Array[String]): Unit = {
    val appName = "NewsSubCategoryProcesser"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val newsPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"

    //------------------------------------1 处理国内二级分类标注数据（4个分类） -----------------------------------------
    // 1.去除three_level 为null
    // 2.抽取three_level 为crime、law
    // 3.抽取two_level 为politics、education
    // 4.其他放为others(量不足)
    def national_data_processer_v1(spark: SparkSession,
                                   newsPath: String,
                                   dt: String="2019-01-08") = {
      val df = spark.read.parquet("/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=2019-01-07")
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]").selectExpr("resource_id as article_id", "html", "title").withColumn("content", getcontentUDF(col("html"))).drop("html")
      val nonull_df = df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time").filter("one_level = 'national'").filter("three_level is not null")
      val class_df = nonull_df.join(ori_df, Seq("article_id"))
      val crime_df = class_df.filter("three_level = 'crime'").drop("two_level").withColumn("two_level", lit("crime"))
      val law_df = class_df.filter("three_level = 'law'").drop("two_level").withColumn("two_level", lit("law"))
      val politics_df = class_df.filter("two_level = 'politics'")
      val education_df = class_df.filter("two_level = 'education'")
      crime_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/crime")
      law_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/law")
      politics_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/politics")
      education_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/education")
    }
    //------------------------------------2 处理国内二级分类标注数据（5个分类） -----------------------------------------
    //  1.去除three_level 为null
    //  2.抽取three_level 和two_level 为crime、law
    //  3.抽取two_level 为politics、education
    //  4.其他放为others(two_level: environment\medical\military\traffic\national economy
    //        three_level:people or groups\accidents\public benefit\events\others)
    def national_data_processer_v2(spark: SparkSession,
                                 newsPath: String,
                                 dt: String="2019-01-08") = {
      val dt = "2019-01-08"
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df =spark.read.parquet(path)
      val getcontentUDF = udf{(html:String) => Jsoup.parse(html).text()}
      val ori_df = spark.read.option("basePath",newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]").selectExpr("resource_id as article_id","html","title").withColumn("content", getcontentUDF(col("html"))).drop("html")
      val nonull_df = df.drop("_class","_id","article_doc_id","is_right","op_time","server_time").filter("one_level = 'national'").filter("three_level is not null")
      val class_df = nonull_df.join(ori_df,Seq("article_id"))
      val crime_df = class_df.filter("two_level = 'crime' or three_level = 'crime'").drop("two_level").withColumn("two_level",lit("crime"))
      val law_df  = class_df.filter("two_level = 'law' or three_level = 'law'").drop("two_level").withColumn("two_level",lit("law"))
      val politics_df = class_df.filter("two_level = 'politics'")
      val education_df = class_df.filter("two_level = 'education'")
      val others_df1 = {
        class_df.filter("two_level = 'society' and three_level in ('others','people or groups','accidents','public benefit','events','food security')")
          .drop("two_level")
          .withColumn("two_level",lit("society others"))
          .select("article_id", "title", "content", "one_level","two_level","three_level","choose_keywords","manual_keywords")
      }
      others_df1.cache
      val others_df2 = {
        class_df.filter("two_level in ('environment','medical','military', 'traffic','national economy')")
          .drop("two_level")
          .withColumn("two_level",lit("society others"))
          .select("article_id", "title", "content", "one_level","two_level","three_level","choose_keywords","manual_keywords")
      }
      others_df2.cache
      val others = others_df1.union(others_df2).distinct()
      println(">>>>>>>>>>正在写入数据")
      crime_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/crime")
      law_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/law")
      politics_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/politics")
      education_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/education")
      others.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/others")
      println(">>>>>>>>>>写入数据完成")
    }

  }

}
