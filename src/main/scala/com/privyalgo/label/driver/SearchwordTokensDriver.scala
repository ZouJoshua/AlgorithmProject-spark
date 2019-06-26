package com.privyalgo.label.driver

import com.privyalgo.label.lexicalanalyzer.{FormTokenDF, SearchWordCoreNLPTokens}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by caifuli on 17-8-16.
  */
object SearchwordTokensDriver {
  def main(args:Array[String]) = {
    var textInputDir: String = null    //JSON形式的文本输入路径
    var stopwordsInputDir: String = null
    var outputDir: String = null
    val len = args.length

    for (i <- 0 until len) {
      val myVar = args(i)
      myVar match {
        case "-tin" => textInputDir = args(i + 1)
        case "-swin" => stopwordsInputDir = args(i + 1)
        case "-out" => outputDir = args(i + 1)
        case _ =>
      }
    }

    if (null == textInputDir || null == stopwordsInputDir || null == outputDir) {
      println("USG: -tin textInputDir -swin stopwordsInputDir -out outputDir")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("searchword text CoreNLP Tokens")
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

    val att = new SearchWordCoreNLPTokens(text, bcStopWords)
    val tokenRes = att.run()
    val tokenDF = new FormTokenDF(tokenRes, sqlc).run()

    tokenDF.write.mode("overwrite").parquet(outputDir)

    sc.stop()
  }

}
