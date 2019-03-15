package com.algo.nlp

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.jsoup.Jsoup


/**
  * Created by Joshua on 2018-11-20
  */

object CleanHtml {
  /**
    * 清洗
    * 1.从html中抽出新闻正文
    * 2.对抽出的新闻正文进行清洗，保持新闻正文原始样子
    */

  def cleanString(str: String): String = {
    /**
     *1.替换掉换行符‘\r\n\t’
     *2.清除掉‘&#13;’‘j&amp;k’等html转义符
     *3.单词间隔只保存一个空格
     *4.去重
     *5.去掉网址
     **/
    if (str == null || str == "" || str.length <= 0 || str.replaceAll(" ", "").length <= 0 || str == "None") {
      null
    } else {
      str.trim().replaceAll("[\r\n\t]", " ")
        .replaceAll("(&amp;|&#13;|&nbsp;)","")
        .replaceAll("[ ]+"," ")
        .replaceAll("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]","")
        .replaceAll("^(\\w+((-\\w+)|(\\.\\w+))*)\\+\\w+((-\\w+)|(\\.\\w+))*\\@[A-Za-z0-9]+((\\.|-)[A-Za-z0-9]+)*\\.[A-Za-z0-9]+$","")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PrepPocess-nlp")
      .getOrCreate()
    import spark.implicits._

    val savepath = "/user/zoushuai/news_content/clean_article"
    val dt = "2018-10-30"

    val news = {
      val path = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=%s/hour=*"
      spark.read.parquet(path.format(dt))
        .selectExpr("resource_id as article_id", "html", "url as article_url", "country_lang as country_lan", "title")
        //        .withColumn("three_level",col("three_level").cast(StringType))
        .repartition(512)
        .dropDuplicates("html")
    }.distinct()
//    }.filter("length(html) > 4500")

    val news_clean = news.rdd.map{
      r =>
        val id = r.getAs[String]("article_id").toString
        val html_str = r.getAs[String]("html").toString
        //Todo：云服务提供用正则抽取新闻正文出现文章标题、发布日期、tags、网站版权等信息
        //Todo:目前从html解析出正文内容时直接取div标签内容（可能会取到正文以外的部分内容），可以结合p标签抽取，同时结合正则替换
        val doc = Jsoup.parse(html_str).select("div").text()
          .replaceAll("<.*?>","").replaceAll("<br/>","")
        val article = cleanString(doc)
        (id,html_str,article)
    }.toDF("article_id", "html", "article")

    news_clean.write.mode(SaveMode.Overwrite).save(savepath + "/dt=%s".format(dt))
    spark.stop()
  }
}
