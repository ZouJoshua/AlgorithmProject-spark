package com.apus.nlp

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.jsoup.Jsoup


/**
  * Created by Joshua on 2018-11-20
  */
object CleanHtml {
  //  清洗
  //  1.内容小写(暂不大小写)
  //  2.替换掉‘\n\t’
  //  3.清除掉‘&#13;’‘j&amp;k’等html转义符
  //  4.单词间隔只保存一个空格
  //  5.去重
  //  val str_p = """[  ]+""".r正则未使用该方法

  def cleanString(str: String): String = {
    if (str == null || str == "" || str.length <= 0 || str.replaceAll(" ", "").length <= 0 || str == "None") {
      null
    } else {
      str.trim().replaceAll("[\r\n\t]", " ")
        .replaceAll("(&amp;|&#13;|&nbsp;)","")
        .replaceAll("[ ]+"," ")
        .replaceAll("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]","")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PrepPocess-nlp")
      .getOrCreate()
    import spark.implicits._

//    val readpath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"
    val savepath = "/user/zoushuai/news_content/clean_article"
    val dt = "2018-10-30"
//    val news = spark.read.option("basePath","/user/hive/warehouse/apus_dw.db/dw_news_data_hour").parquet(readpath + "/dt=%1$s*".format(dt)).select("resource_id","html").distinct

    val news = {
      val path = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=%s/hour=*"
      spark.read.parquet(path.format(dt))
        .selectExpr("resource_id as article_id", "html", "url as article_url", "country_lang as country_lan", "title")
        //        .withColumn("three_level",col("three_level").cast(StringType))
        .repartition(512)
        .dropDuplicates("html")
    }
//    }.filter("length(html) > 4500")

    val news_clean = news.rdd.map{
      r =>
        val id = r.getAs[String]("article_id").toString
        val html_str = r.getAs[String]("html").toString
        //Todo:解析html为内容时直接取div标签内容的话，会取到文章以外的部分，可以结合p标签抽取
        val doc = Jsoup.parse(html_str).select("div").text()
          .replaceAll("<.*?>","").replaceAll("<br/>","")
        val article = cleanString(doc)
        (id,html_str,article)
    }.toDF("article_id", "html", "article")

    news_clean.write.mode(SaveMode.Overwrite).save(savepath + "/dt=%s".format(dt))
    spark.stop()
  }
}
