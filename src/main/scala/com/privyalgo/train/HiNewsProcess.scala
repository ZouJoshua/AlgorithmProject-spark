package com.privyalgo.train

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.jsoup.Jsoup

/**
  * Created by Joshua on 19-5-8
  */


object HiNewsProcess {
  def main(args: Array[String]): Unit = {

    val appName = "Hi-News-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext

    //***********//
    //印度语新闻处理//
    //***********//

    val hi_news_path = "/user/hive/warehouse/apus_ai.db/recommend/common/news_hindi_parsed"
    // 解析分类获取的top—category
//    val hi_category_path = "hi_news/result_topcategory_all"
//    val hi_category_out_path = "hi_news/topcategory_all"
    // 解析网址获取的top-category
    val hi_category_path = "hi_news/result_topcategory_website_all"
    val hi_category_out_path = "hi_news/topcategory_website_all"
    val getcontentUDF = udf{ html: String => Jsoup.parse(html).text()}
    val hi_df = {
      spark.read.parquet(hi_news_path)
        .filter("country = 'IN' and lang = 'hi'")
        .selectExpr("resource_id as id", "url", "title", "article.html as html", "category")
        .withColumn("content", getcontentUDF(col("html")))
        .drop("html")
    }

//    val hi_category_df = spark.read.json(hi_category_path).selectExpr("id","top_category","result.tag as tags")
    val hi_category_df = spark.read.json(hi_category_path).selectExpr("id","top_category")

    val hi_category = hi_category_df.join(hi_df,Seq("id"))
    hi_category.repartition(1).write.format("json").mode("overwrite").save(hi_category_out_path)

    //*************************//
    //单独处理tech、auto、science//
    //*************************//
    val df1 = spark.read.json("hi_news/result_topcategory_all").filter("top_category in ('technology', 'auto', 'science')").select("id", "top_category")
    val df2 = spark.read.json("hi_news/topcategory_website_all").filter("top_category in ('technology', 'auto', 'science')").select("id", "top_category")

    val df_union = df1.union(df2).distinct().dropDuplicates("id")
    val hi_df_less =df_union.join(hi_df,Seq("id"))
    hi_df_less.repartition(1).write.format("json").mode("overwrite").save("hi_news/auto_science_tech")

  }
}
