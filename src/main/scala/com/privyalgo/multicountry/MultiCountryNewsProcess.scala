package com.privyalgo.multicountry

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.jsoup.Jsoup

/**
  * Created by Joshua on 19-5-8
  */


object MultiCountryNewsProcess {
  def main(args: Array[String]): Unit = {

    val appName = "Multi-Country-News-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext

    //**************//
    //多国家英语新闻处理//
    //**************//

    val all_data_path = "/user/zoushuai/news_content/readmongo/multi_country_news/dt=2019-06-04/country=*"

    val getcontentUDF = udf{ html: String => Jsoup.parse(html).text()}
    val mulit_df = {
      spark.read.parquet(all_data_path)
        .selectExpr("resource_id as id", "url", "title", "article.html as html", "country","lang")
        .filter("html is not null")
        .filter("title is not null")
        .withColumn("content", getcontentUDF(col("html")))
        .drop("html")
    }

    // 多国语言内容保存
    mulit_df.filter("length(content) > 50").repartition(1).write.format("json").mode("overwrite").save("multi_country_news/raw_train_data")

  }
}
