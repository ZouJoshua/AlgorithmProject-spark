package com.privyalgo.article

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * Created by Joshua on 19-8-8
  */
object SensitiveWordsStatic {

  def main(args: Array[String]): Unit = {
    val appName = "Sensitive-Words-Static"
    val spark = SparkSession.builder()
      .master("local")
      .appName(appName)
      .getOrCreate()
    val sc = spark.sparkContext

    val sensitive_words_file = "news_content/en_sensitive_words.txt"
    val article_path = "/user/hive/warehouse/apus_ai.db/recommend/article/article_profile_byRequest/dt=2019-07-*"
    val words_list = spark.read.text(sensitive_words_file).select(collect_list("value") as "words_list").head.getAs[Seq[String]]("words_list")


    val df = spark.read.parquet(article_path).select("id","title","content")

    val sensitiveUDF = udf {
      (text: String) =>
        var sensitive_list = Seq.empty[String]
        val text_token = text.toLowerCase.split(" ")
        words_list.foreach{
          w =>
//            val w_ = if(text.toLowerCase.contains(" "+ w.toLowerCase)|text.toLowerCase.contains(w.toLowerCase+" ")|text.toLowerCase.contains(w.toLowerCase)) w else ""
            val w_ = if(text_token.contains(w.toLowerCase)) w else ""
            if(w_ !=""){
              sensitive_list = sensitive_list :+ w_
            }
        }
        sensitive_list.toList
    }
    val result = {
      df.withColumn("title_sensitive", sensitiveUDF(col("title")))
        .withColumn("content_sensitive", sensitiveUDF(col("content")))
        .selectExpr("id","title","content", "title_sensitive", "size(title_sensitive) as title_sensitive_len",
          "content_sensitive", "size(content_sensitive) as content_sensitive_len")
    }

    val words_result = {
      result.withColumn("s_word", explode(col("content_sensitive")))
    }

    result.filter("size(content_sensitive) > 0").groupBy("content_sensitive_len").count.sort(desc("count")).show
    words_result.groupBy("s_word").count.sort(desc("count")).show
  }
}
