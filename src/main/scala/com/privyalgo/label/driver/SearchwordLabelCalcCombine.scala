package com.privyalgo.label.driver

import com.privyalgo.label.keywordextractor.{SearchwordLabelRake, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by caifuli on 17-7-25.
  */
object SearchwordLabelCalcCombine {
  def main(args: Array[String]): Unit = {
    var searchwordTokensInput: String = null
    var searchwordTextInput: String = null
    var stopWordsInput: String = null
    var tfThres: String = null
    var idfPercent: String = null
    var outputDir: String = null
    val len = args.length

    for (i <- 0 until len) {
      val myVar = args(i)
      myVar match {
        case "-searchwordTokenIn" => searchwordTokensInput = args(i + 1)
        case "-textIn" => searchwordTextInput = args(i + 1)
        case "-stopwords" => stopWordsInput = args(i + 1)
        case "-tf" => tfThres = args(i + 1)
        case "-idfPercent" => idfPercent = args(i + 1)
        case "-out" => outputDir = args(i + 1)
        case _ =>
      }
    }

    if (null == searchwordTokensInput || null == outputDir || null == searchwordTextInput || null == stopWordsInput || null == tfThres || null == idfPercent) {
      println("USG: -searchwordTokenIn searchwordTokensInput -textIn searchwordTextInput -stopwords stopWordsInput -tf tfThres -idfPercent idfPercent -out outputDir")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("searchword label calc and combine")
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)
    import sqlc.implicits._

    val tokens = sqlc.read.parquet(searchwordTokensInput)
    val text = sc.textFile(searchwordTextInput).repartition(1024)
    val stop = sc.textFile(stopWordsInput)
    val wordArr = stop.collect()
    var stopwordsSet: Set[String] = Set[String]()
    for (word <- wordArr) {
      stopwordsSet += word
    }
    val bcStopWords = sc.broadcast(stopwordsSet)

    /* 利用tf-idf计算app的标签 */
    val ltf = new TFIDF(tokens, sqlc, tfThres, idfPercent)
    val tfDF = ltf.run().dropDuplicates(Seq("tf_pn"))
    //tfDF.write.mode("overwrite").parquet("/user/caifuli/searchwords_tags/20170724/searchwords_tags/tfidf")

    /* 利用textrank算法计算app标签 */
    val ltr = new TextRank(tokens, sqlc)
    val trDF = ltr.run().dropDuplicates(Seq("tr_pn"))
    //trDF.write.mode("overwrite").parquet("/user/caifuli/searchwords_tags/20170724/searchwords_tags/textrank")

    /*利用rake算法计算app标签*/
    val utlr = new SearchwordLabelRake(text,bcStopWords)
    val rakeDF = utlr.run().toDF("rake_pn","rake_tags").dropDuplicates(Seq("rake_pn"))
    //rakeDF.write.mode("overwrite").parquet("/user/caifuli/searchwords_tags/20170724/searchwords_tags/rake")

    /* 合并tf-idf、textrank和rake算法分别计算出来的标签 */
    val lc = new LabelCombine(trDF, rakeDF, tfDF, sqlc)
    val combineDF = lc.run()

    combineDF.write.mode("overwrite").parquet(outputDir)

    sc.stop()
  }

}
