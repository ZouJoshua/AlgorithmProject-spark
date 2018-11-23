package com.apus.nlp

import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by Joshua on 2018-11-23
  */

object NewsLDA {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("PrepPocess-nlp")
      .getOrCreate()
    val sc = new SparkContext()
    import spark.implicits._

    val ngrams = "/user/zoushuai/article_wiki"
    // Loads data.
    val dataset = spark.read.parquet(ngrams)

    // Trains a LDA model.
    val lda = new LDA().setK(200).setMaxIter(10)
    val model = lda.fit(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    // Shows the result.
    val transformed = model.transform(dataset)
    transformed.show(false)
  }
}
