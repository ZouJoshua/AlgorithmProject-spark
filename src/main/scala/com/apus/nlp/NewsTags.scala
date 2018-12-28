package com.apus.nlp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, TextNode}

/**
  * Created by Joshua on 2018-12-27
  */
object NewsTags {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ArticleInfoProcess-mongodb")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val newsPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"

    //------------------------------------1 读取数据-----------------------------------------
    // 解析所有文章内容
    val getcontentUDF = udf{(html:String) => Jsoup.parse(html).text()}
    val articleDF = {
      val seqUDF = udf((t: String) => Seq.empty[String])
      spark.read.option("basePath",newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
        .selectExpr("resource_id as article_id", "html", "tags","url as article_url")
        .withColumn("article", getcontentUDF(col("html")))
        .dropDuplicates("html","article")
        .filter("country_lang = 'IN_en'")
    }

    //------------------------------------2 处理抓取的tags-----------------------------------------

    val tagDF = {
        articleDF.map{
          r =>
            val id = r.getAs[String]("article_id")
            val tags_array = r.getAs[String]("tags").replace("[","").replace("]","").split(",")
            val tags_array_lower = tags_array.map(_.toLowerCase)
            val tg_count = tags_array.length
            (id, tags_array, tags_array_lower, tg_count)
        }
    }.toDF("article_id", "tags_array", "tags_array_lower", "tg_count")

    //------------------------------------3 处理超链接的tags-----------------------------------------
    // 正则提取超链接 存在a标签嵌套问题，用jsoup解析提取
    val hrefDF = {
      articleDF.map{
        row =>
          val id = row.getAs[String]("article_id")
          val url = row.getAs[String]("article_url")
          val html = row.getAs[String]("html")
//          val href_array = """(href=".*?")|(href='.*?')"""
//          val href_array = """(<a href=".*?".*>.*?</a>)|(<a href=".*?".*>.*?</a>)""".r.findAllIn(html).toSeq
          val doc = Jsoup.parse(html)
          val tags_array = doc.select("a").eachText().toArray.map(_.toString.trim).toSeq
          val href_array = doc.select("a").eachAttr("href").toArray.map(_.toString.trim).toSeq
          val hr_count = href_array.length
          (id, url, tags_array, href_array, hr_count)
      }.toDF("article_id","article_url", "tags_array", "href_array", "hr_count")
    }

    // 提取超链接tags到集合
    val href_tags_all = hrefDF.filter("hr_count > 0").rdd.flatMap(r => r.getAs[Seq[String]]("tags_array")).collect //.toSet
    //    sc.parallelize(href_tags_all.toList).saveAsTextFile("news_content/out.txt")
    val tags_count = sc.parallelize(href_tags_all.toList).map((_, 1)).reduceByKey(_ + _).toDF("tags","count")

    val tags_filter_count = {
      tags_count.map {
        r =>
          val tags = r.getAs[String]("tags")
          val count = r.getAs[Int]("count")
          val tags_len = tags.split(" ").length
          (tags, count, tags_len)
      }.toDF("tags", "count", "tags_len")
    }
    //    tags_filter_count.filter("tags_len < 5").sort(desc("count")).show(30, false)
    //    tags_filter_count.filter("tags_len < 5").sort("count").show(30, false)
    val tags_out = tags_filter_count.filter("tags not like '%http%'").filter("tags not like '%@%'").filter("tags_len < 5").sort(desc("count")) //.show(30, false)
    tags_out.select("tags").coalesce(1).write.mode("overwrite").text("news_content/tmp/out")

      val href_tags_file = {
        hrefDF.filter("hr_count > 0").map{
          row =>
            val id = row.getAs[String]("article_id")
            val url = row.getAs[String]("article_url")
            val href_array = row.getAs[Seq[String]]("href_array").toString
            val tags_array = row.getAs[Seq[String]]("tags_array").toString
            val text = id + "\t" + url + "\t" + href_array + "\t" + tags_array
            text
        }.toDF("tags")
      }
        //    href_tags_file.coalesce(1).write.mode("overwrite").text("news_content/tmp/out")

    //------------------------------------4 处理url提取tags-----------------------------------------
    // 处理url特征

    //------------------------------------5 合并数据-----------------------------------------

    val article = articleDF.join(tagDF,Seq("article_id")).drop("tags")
        article.cache
    }
}
