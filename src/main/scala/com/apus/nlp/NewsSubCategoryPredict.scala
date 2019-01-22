package com.apus.nlp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.jsoup.Jsoup

/**
  * Created by Joshua on 2019-01-22
  */
object NewsSubCategoryPredict {
  def main(args: Array[String]): Unit = {
    val appName = "NewsSubCategoryPredict"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
//    val path = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=2019-01-11"
    val path1 = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=2019-01-1[1-9]"
    val path2 = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=2019-01-2[0-9]"

    val categoryMap = Map(
      "0"->"the latest",
      "1"->"top stories",
      "3"->"international",
      "4"->"national",
      "5"->"politics",
      "6"->"sports",
      "7"->"auto or science",
      "8"->"business",
      "9"->"science",
      "10"->"technology",
      "11"->"auto",
      "12"->"lifestyle",
      "14"->"astrology",
      "15"->"entertainment",
      "17"->"education",
      "18"->"arts",
      "19"->"health",
      "20"->"travel",
      "21"->"games",
      "22"->"cricket",
      "23"->"video",
      "24"->"photos",
      "25"->"employment",
      "26"->"buzz",
      "27"->"bikini",
      "31"->"gallery",
      "56"->"food&drink",
      "60"->"ramadan",
      "100"->"for you",
      "210"->"world cup",
      "205"->"adulto",
      "206"->"fun",
      "300"->"video",
      "500"->"curiosidades"
    )

    val contentUDF = udf((html:String,text:String) => if(text!="") text else if(html!="") Jsoup.parse(html).text() else "")
    val trimUDF = udf((inp:String) => inp.replaceAll("\\s+"," "))
    val df1 = {
      spark.read.parquet(path1)
        .selectExpr("resource_id as article_id","title","article", "url", "category")
        .withColumn("content",trimUDF(contentUDF($"article.html",$"article.text")))
        .drop("article")
    }
    val df2 = {
      spark.read.parquet(path2)
        .selectExpr("resource_id as article_id", "title", "article", "url", "category")
        .withColumn("content",trimUDF(contentUDF($"article.html",$"article.text")))
        .drop("article")
    }
    val df = df1.union(df2).distinct()

    val df_res = {
      df.map{
        row =>
          val id = row.getAs[Int]("article_id").toString
          val url = row.getAs[String]("url")
          val title = row.getAs[String]("title")
          val content = row.getAs[String]("content")
          val one_level = categoryMap.getOrElse(row.getAs[String]("category"), "others")
          (id, url, title, content, one_level)
      }.toDF("article_id", "url", "title", "content", "one_level")
    }
    val out = df_res.filter("one_level in ('international', 'national','sports','auto or science', 'business', 'technology', 'auto', 'lifestyle', 'entertainment', 'science')").filter("content != ''")
    out.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/predict/predict_all")
  }
}
