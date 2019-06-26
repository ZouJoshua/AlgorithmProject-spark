package com.privyalgo.label.driver

import com.privyalgo.label.lexicalanalyzer.{FormTokenDF, URLTextCoreNLPTokens}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by caifuli on 17-8-16.
  */
object URLTokensDriver {
  def main(args:Array[String]) = {
    var textInputDir: String = null    //JSON形式的文本输入路径
    var stopwordsInputDir: String = null
    var outputDir: String = null
    var tfThres: String = null
    val len = args.length

    for (i <- 0 until len) {
      val myVar = args(i)
      myVar match {
        case "-tin" => textInputDir = args(i + 1)
        case "-swin" => stopwordsInputDir = args(i + 1)
        case "-tf" => tfThres = args(i + 1)
        case "-out" => outputDir = args(i + 1)
        case _ =>
      }
    }

    if (null == textInputDir || null == stopwordsInputDir || null == outputDir || null == tfThres) {
      println("USG: -tin textInputDir -swin stopwordsInputDir -tf tfThres -out outputDir")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("url text CoreNLP Tokens")
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)

    val text = sc.textFile(textInputDir).repartition(1024)
    val stop = sc.textFile(stopwordsInputDir)
    val wordArr = stop.collect()
    var stopwordsSet: Set[String] = Set[String]()
    for (word <- wordArr) {
      stopwordsSet += word
    }
    val bcStopWords = sc.broadcast(stopwordsSet)

    val att = new URLTextCoreNLPTokens(text, bcStopWords, tfThres)    //todo:20170620的app数据的tfThres的阈值设为3，是因为有一部分脏数据
    val tokenRes = att.run()
    val tokenDF = new FormTokenDF(tokenRes, sqlc).run()

    tokenDF.write.mode("overwrite").parquet(outputDir)

    sc.stop()
  }

}
