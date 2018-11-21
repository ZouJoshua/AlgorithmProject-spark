package com.apus.nlp

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.clulab.processors.Document
import org.clulab.processors.fastnlp.FastNLPProcessor

/**
  * Created by Joshua on 2018-11-20
  */
object PreProcess {

  def cleanString(str: String): String = {
    if (str == null || str == "" || str.length <= 0 || str.replaceAll(" ", "").length <= 0 || str == "None") {
      null
    } else {
      str.toLowerCase.trim().replaceAll("[\r\n\t]", " ")
        .replaceAll("(&amp;|&#13;|&nbsp;)","")
        .replaceAll("[ ]+"," ")
        .replaceAll("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]","")
    }
  }

  def get_Ngrams(words:Seq[String], minSize: Int = 1, maxSize: Int = 4):Seq[String] ={
    var Ngrams_result = Seq.empty[String]
    for(j <- minSize to maxSize){
      for(i <- words.indices){
        val a = i
        val b = if(i+j < words.size) i+j else words.size
        if(words.slice(a,b).length == j){
          Ngrams_result = Ngrams_result :+ words.slice(a,b).mkString(" ")
        }
      }
    }
    Ngrams_result
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PrepPocess-nlp")
      .getOrCreate()
    val sc = new SparkContext()
    import spark.implicits._

    val dt = "2018-10"
    val newspath = "/user/zoushuai/news_content/clean_article/dt=%s"
    val wikipath = "/user/zoushuai/wiki_title"

//    val wikiDF = spark.read.parquet(newspath.format(dt)).distinct
//    val news = spark.read.option("basePath","/user/hive/warehouse/apus_dw.db/dw_news_data_hour").parquet(readpath + "/dt=%s*".format(dt)).select("resource_id","html").distinct

    val articleDF = {
      spark.read.parquet(newspath.format(dt))
        .selectExpr("resource_id as article_id","article")
//        .withColumn("three_level",col("three_level").cast(StringType))
        .repartition(512)
        .dropDuplicates("article")
    }.filter("length(article) > 100").limit(100)
    articleDF.cache

    val wiki = spark.read.parquet(wikipath).limit(100).map(r => cleanString(r.getAs[String]("title"))).collect()
    val wiki_bd = sc.broadcast(wiki)

    val art_wiki_df = articleDF.sample(false, 0.1).filter("article is not null").map{
      r =>
        val id = r.getAs[String]("article_id")
        val text = cleanString(r.getAs[String]("article"))
        val split_word = {
          text.replaceAll("(\\s*\\p{P}\\s+|\\s+\\p{P}\\s*)"," ")
            .replaceAll("\\s+", " ")
            .split(" ").toSeq
        }
        val wordsNgram = get_Ngrams(split_word, 1, 4).toSet
        val output = wiki_bd.value.toSet.intersect(wordsNgram).toSeq
        (id,output)
    }.toDF("article_id","entity_keywords")

//    val wiki = spark.read.parquet(wikipath).map(x => (x.getAs[String]("title"),"1")).collect().toMap.filterKeys(_.contains("Bibby, Thomas"))
//    val dict_bd = sc.broadcast(wiki)

    val file = "/user/caifuli/new_stopwords.txt"
    def get_Stopwords(file:String): Set[String] ={
      val stop = sc.textFile(file)
      val wordArr = stop.collect()
      var stopwordsSet: Set[String] = Set[String]()
      for (word <- wordArr) {
        stopwordsSet += word
      }
      stopwordsSet
    }

    val stopwords_bd = sc.broadcast(get_Stopwords(file))

    val Ngrams_df = articleDF.rdd.map{
      r =>
        val proc = new FastNLPProcessor()
        val id = r.getAs[String]("article_id")
        val text = r.getAs[String]("article")
        val doc: Document = proc.mkDocument(text)
        proc.tagPartsOfSpeech(doc)
        proc.lemmatize(doc)
        doc.clear()
        val lemmas = doc.sentences.flatMap{
          sentence =>
            sentence.lemmas.getOrElse(Seq.empty[String]).asInstanceOf[Array[String]]
        }.filter(x => !stopwords_bd.value.contains(x))
        val ngrams = get_Ngrams(lemmas)
        (id,text,ngrams)
    }.toDF("article_id", "article", "n_grams")
    Ngrams_df.cache

    val tagUDF = udf{
      (article:String, words:Seq[String]) =>
        var tmp_art = " " + article + " "
        words.foreach{
          w =>
            tmp_art = tmp_art.replaceAll("\\s(?i)" + w + "(?=\\s)$", " <i class=\"apus-entity-words\">" + w + "</i> ")
        }
        tmp_art
    }

    val art_dict_df = articleDF.sample(false, 0.1).filter("article is not null").map{
      r =>
        val id = r.getAs[String]("article_id")
        val text = r.getAs[String]("article")
        val split_word = {
          text.replaceAll("(\\s*\\p{P}\\s+|\\s+\\p{P}\\s*)"," ")
            .replaceAll("\\s+", " ")
            .split(" ").toSeq
        }
        val words_Ngram = get_Ngrams(split_word, 1, 4).toSet
        val output = wiki_bd.value.toSet.intersect(words_Ngram).toSeq
        (id,output)
    }.toDF("article_id","entity_keywords")

    news_clean.write.mode(SaveMode.Overwrite).save(savepath + "/dt=" + dt)
    spark.stop()
  }
}
