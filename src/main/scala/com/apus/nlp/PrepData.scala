package com.apus.nlp

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by Joshua on 2018-11-24
  */
object PrepData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PrepData-nlp")
      .getOrCreate()
    val sc = new SparkContext()
    import spark.implicits._

    val ngramspath = "/user/zoushuai/news_content/docword/dt=2018-11-20"
    val vocabpath = "/user/zoushuai/news_content/vocab/dt=2018-11-20"

    val docword_df = spark.read.parquet(ngramspath)

    // 生成UCI格式word文件
    val word_UCI = docword_df.flatMap{
      r =>
        val docID = r.getAs[String]("article_id")
        val ngrams = r.getAs[Seq[Long]]("content_ngram_idx")
        var out = Seq.empty[(String, Long)]
        for(wordID <- ngrams){
          out = out :+ (docID.toString,wordID)
        }
        out
    }.toDF("docID","wordID")

    // 生成文章词库
    val vocab_df = spark.read.parquet(vocabpath)
    val vocab = vocab_df.map{
      x =>
        val word_id = x.getAs[String]("value").split("\t")
        (word_id(0).trim(), word_id(1).toLong)
    }.toDF("word","wordID")

    // 保存文件
    val word_save = word_UCI.groupBy("docID", "wordID").agg(count("wordID").as("tf"))
    val vocab_save = vocab.sort("wordID").select("word")

    word_save.repartition(1).write.mode(SaveMode.Overwrite).text("news_content/word_uci/dt=2018-11-20")
    vocab_save.repartition(1).write.mode(SaveMode.Overwrite).text("news_content/word_vocab/dt=2018-11-20")
  }
}
