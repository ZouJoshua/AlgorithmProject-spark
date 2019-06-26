package com.privyalgo.label.driver

import com.privyalgo.label.keywordextractor._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * app标签计算(tf-idf,textrank,rake)与合并的驱动类
  * Created by caifuli on 17-7-21.
  */
object AppsLabelCalcCombine {
  def main(args: Array[String]): Unit = {
    var appTokensInput: String = null    //分词DF结果存储路径
    var appTextDFInput: String = null    //GP结果DF存储路径
    var stopWordsInput: String = null
    var tfThres: String = null
    var idfPercent: String = null
    var outputDir: String = null
    val len = args.length

    for (i <- 0 until len) {
      val myVar = args(i)
      myVar match {
        case "-appTokenIn" => appTokensInput = args(i + 1)
        case "-textIn" => appTextDFInput = args(i + 1)
        case "-stopwords" => stopWordsInput = args(i + 1)
        case "-tf" => tfThres = args(i + 1)
        case "-idfPercent" => idfPercent = args(i + 1)
        case "-out" => outputDir = args(i + 1)
        case _ =>
      }
    }

    if (null == appTokensInput || null == outputDir || null == appTextDFInput || null == stopWordsInput || null == tfThres || null == idfPercent) {
      println("USG: -appTokenIn appTokenInput -textIn appTextInput -stopwords stopWordsInput -tf tfThres -idfPercent idfPercent -out outputDir")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("app label calc and combine")
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)
    import sqlc.implicits._

    val tokens = sqlc.read.parquet(appTokensInput)
    val text = sqlc.read.parquet(appTextDFInput)
    val stop = sc.textFile(stopWordsInput)
    val wordArr = stop.collect()
    var stopwordsSet: Set[String] = Set[String]()
    for (word <- wordArr) {
      stopwordsSet += word
    }
    val bcStopWords = sc.broadcast(stopwordsSet)

    /* 利用tf-idf计算app的标签 */
    /*val ltf = new TFIDF(tokens, sqlc, tfThres, idfPercent)
    val tfDF = ltf.run()
    //tfDF.write.mode("overwrite").parquet("/user/caifuli/app_tags/app_tags/20170717/tfidf")

    /* 利用textrank算法计算app标签 */
    val ltr = new TextRank(tokens, sqlc)
    val trDF = ltr.run()*/
    //trDF.write.mode("overwrite").parquet("/user/caifuli/app_tags/app_tags/20170717/textrank")


    /*利用rake算法计算app标签*/
    val atlr = new AppsTextLabelRake(text, bcStopWords)
    val rakeDF = atlr.run().toDF("rake_pn","rake_tags")
    rakeDF.write.mode("overwrite").parquet("/user/caifuli/app_tags/app_tags/20170717/rake")

    /* 合并tf-idf和textrank算法分别计算出来的标签 */
    val trDF = sqlc.read.parquet("/user/caifuli/app_tags/app_tags/20170717/textrank")
    //val rakeDF = sqlc.read.parquet("/user/caifuli/app_tags/app_tags/20170717/rake")
    val tfDF = sqlc.read.parquet("/user/caifuli/app_tags/app_tags/20170717/tfidf")
    val lc = new LabelCombine(trDF, rakeDF, tfDF, sqlc)
    val combineDF = lc.run()

    combineDF.write.mode("overwrite").parquet(outputDir)

    sc.stop()
  }

}
