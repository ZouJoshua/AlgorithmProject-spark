package com.apus.mongodb

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.clulab.processors.Document
import org.clulab.processors.fastnlp.FastNLPProcessor
import org.clulab.struct.DirectedGraphEdgeIterator
import org.jsoup.Jsoup


/**
  * Created by Joshua on 2018-12-29
  */
object TestFastNLP {
  def main(args: Array[String]): Unit = {
    val appName = "TestFastNLP"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext


    val dt = "2018-11-22"
    val dataDtPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=%s".format(dt)

    val inpDF = {
      val html_udf = udf { (html: String) => Jsoup.parse(html).text() }
      spark.read.parquet(dataDtPath).filter("country_lang = 'IN_en'")
        .selectExpr("resource_id as article_id", "url as article_url", "html", "title", "tags")
        .withColumn("article", html_udf(col("html")))
        .dropDuplicates("article")
    }

    val text = inpDF.head.getAs[String]("article")
    val proc = new FastNLPProcessor()
    val doc: Document = proc.mkDocument(text)

    var sentenceCount = 0
    for (sentence <- doc.sentences) {
      println("Sentence #" + sentenceCount + ":")
      println("Tokens: " + sentence.words.mkString(" "))
//      println("Start character offsets: " + sentence.startOffsets.mkString(" "))
//      println("End character offsets: " + sentence.endOffsets.mkString(" "))

      // these annotations are optional, so they are stored using Option objects, hence the foreach statement
      sentence.lemmas.foreach(lemmas => println(s"Lemmas: ${lemmas.mkString(" ")}"))
      sentence.tags.foreach(tags => println(s"POS tags: ${tags.mkString(" ")}"))
      sentence.chunks.foreach(chunks => println(s"Chunks: ${chunks.mkString(" ")}"))
      sentence.entities.foreach(entities => println(s"Named entities: ${entities.mkString(" ")}"))
      sentence.norms.foreach(norms => println(s"Normalized entities: ${norms.mkString(" ")}"))
      sentence.dependencies.foreach(dependencies => {
        println("Syntactic dependencies:")
        val iterator = new DirectedGraphEdgeIterator[String](dependencies)
        while(iterator.hasNext) {
          val dep = iterator.next
          // note that we use offsets starting at 0 (unlike CoreNLP, which uses offsets starting at 1)
          println(" head:" + dep._1 + " modifier:" + dep._2 + " label:" + dep._3)
        }
      })
      sentence.syntacticTree.foreach(tree => {
        println("Constituent tree: " + tree)
        // see the org.clulab.utils.Tree class for more information
        // on syntactic trees, including access to head phrases/words
      })

      sentenceCount += 1
      println("\n")
    }
  }
}
