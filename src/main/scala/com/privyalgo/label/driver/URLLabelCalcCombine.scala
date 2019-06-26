package com.privyalgo.label.driver

import com.privyalgo.label.keywordextractor._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by caifuli on 17-7-24.
  */
object URLLabelCalcCombine {
  def main(args: Array[String]): Unit = {
    var urlTokensInput: String = null
    var urlTextInput: String = null
    var stopWordsInput: String = null
    var tfThres: String = null
    var idfPercent: String = null
    var outputDir: String = null
    val len = args.length

    for (i <- 0 until len) {
      val myVar = args(i)
      myVar match {
        case "-urlTokenIn" => urlTokensInput = args(i + 1)
        case "-textIn" => urlTextInput = args(i + 1)
        case "-stopwords" => stopWordsInput = args(i + 1)
        case "-tf" => tfThres = args(i + 1)
        case "-idfPercent" => idfPercent = args(i + 1)
        case "-out" => outputDir = args(i + 1)
        case _ =>
      }
    }

    if (null == urlTokensInput || null == outputDir || null == urlTextInput || null == stopWordsInput || null == tfThres || null == idfPercent) {
      println("USG: -urlTokenIn urlTokensInput -textIn urlTextInput -stopwords stopWordsInput -tf tfThres -idfPercent idfPercent -out outputDir")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("url label calc and combine")
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)
    import sqlc.implicits._

    val tokens = sqlc.read.parquet(urlTokensInput)
    val text = sc.textFile(urlTextInput).repartition(1024)
    val stop = sc.textFile(stopWordsInput)
    val wordArr = stop.collect()
    var stopwordsSet: Set[String] = Set[String]()
    for (word <- wordArr) {
      stopwordsSet += word
    }
    val bcStopWords = sc.broadcast(stopwordsSet)

    /* 利用tf-idf计算app的标签 */
    val ltf = new TFIDF(tokens, sqlc, tfThres, idfPercent)
    val tfDF = ltf.run()
    //tfDF.write.mode("overwrite").parquet("/user/caifuli/URL_tags/20170721/URL_tags/tfidf")

    /* 利用textrank算法计算app标签 */
    val ltr = new TextRank(tokens, sqlc)
    val trDF = ltr.run()
    //trDF.write.mode("overwrite").parquet("/user/caifuli/URL_tags/20170721/URL_tags/textrank")

    /*利用rake算法计算app标签*/
    val utlr = new URLTextLabelRake(text,bcStopWords)
    val rakeDF = utlr.run().toDF("rake_pn","rake_tags")
    //rakeDF.write.mode("overwrite").parquet("/user/caifuli/URL_tags/20170721/URL_tags/rake")

    /* 合并tf-idf和textrank算法分别计算出来的标签 */
    val lc = new LabelCombine(trDF, rakeDF, tfDF, sqlc)
    val combineDF = lc.run()

    combineDF.write.mode("overwrite").parquet(outputDir)

    sc.stop()
  }

}
