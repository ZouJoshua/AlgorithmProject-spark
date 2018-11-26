package com.apus.nlp

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
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

    val ngramsPath = "/user/zoushuai/news_content/docword/dt=2018-11-20"
    val vocabPath = "/user/zoushuai/news_content/vocab/dt=2018-11-20"
    val tfidfPath = "/user/zoushuai/news_content/tf_idf/dt=2018-11-20"

    val docword_df = spark.read.parquet(ngramsPath)

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

    // 取tf_idf值大于某一阈值的词
    val docword_tfidf_df = spark.read.parquet(tfidfPath)

    val word_tfidf_UCI = docword_tfidf_df.flatMap{
      r =>
        val docID = r.getAs[String]("article_id")
//        val tfidf_ngrams = r.getAs[Seq[(Long,Double)]]("tf_Mul_idf")
        val tfidf_ngrams:Seq[(Long,Double)] = r.getAs[Seq[Row]]("tf_Mul_idf").map(x => {(x.getLong(0), x.getDouble(1))})
        var out = Seq.empty[(String, Long, Double)]
        for(tfidf <- tfidf_ngrams){
          out = out :+ (docID.toString, tfidf._1, tfidf._2)
        }
        out
    }.toDF("docID","wordID","wordTFIDF")

    // 生成文章词库
    val vocab_df = spark.read.parquet(vocabPath)
    val vocab = vocab_df.map{
      x =>
        val word_id = x.getAs[String]("value").split("\t")
        (word_id(0).trim(), word_id(1).toLong)
    }.toDF("word","wordID")

    val word_save_tmp = word_UCI.groupBy("docID", "wordID").agg(count("wordID").as("tf"))
    val word_filtered = word_tfidf_UCI.drop("wordTFIDF").groupBy("docID", "wordID").agg(count("wordID").as("tf"))
    val vocab_save = vocab.sort("wordID").select("word")
//    val vocab_save = vocab.sort(desc("wordID")).select("word")

   // 生成libsvm格式数据
    val word_libsvm_rdd = word_save_tmp.rdd.map{
      r =>
        val did = r.getAs[String]("docID")
        val wid = r.getAs[Long]("wordID")
        val tf = r.getAs[Long]("tf")
        (did.toString, wid + ":" + tf)
    }

    val word_libsvm = word_libsvm_rdd.reduceByKey(_ + " " + _).map(r => r._1.toString + "\t" + r._2 + "\n").toDF("data")

    val word_save = word_save_tmp.map{
      r =>
        val did = r.getAs[String]("docID")
        val wid = r.getAs[Long]("wordID")
        val tf = r.getAs[Long]("tf")
        val txt = did + "|" + wid + "|" + tf
        txt.toString
    }

    val word_filtered_save = word_filtered.map{
      r =>
        val did = r.getAs[String]("docID")
        val wid = r.getAs[Long]("wordID")
        val tf = r.getAs[Long]("tf")
        val txt = did + "|" + wid + "|" + tf
        txt.toString
    }
    // 保存文件
    word_save.repartition(1).write.mode(SaveMode.Overwrite).text("news_content/word_uci/dt=2018-11-20")
    word_filtered_save.repartition(1).write.mode(SaveMode.Overwrite).text("news_content/word_filtered_uci/dt=2018-11-20")
    vocab_save.repartition(1).write.mode(SaveMode.Overwrite).text("news_content/word_vocab/dt=2018-11-20")
    word_libsvm.repartition(1).write.mode(SaveMode.Overwrite).text("news_content/word_libsvm/dt=2018-11-20")
  }
}
