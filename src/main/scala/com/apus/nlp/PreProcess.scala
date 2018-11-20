package com.apus.nlp

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
  * Created by Joshua on 2018-11-20
  */
object PreProcess {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PrepPocess-nlp")
      .getOrCreate()
    val sc = new SparkContext()
    import spark.implicits._

    val readpath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"
    val savepath = "/user/zoushuai/news_content/clean_article"
    val dt = "2018-10-30"
//    val news = spark.read.option("basePath","/user/hive/warehouse/apus_dw.db/dw_news_data_hour").parquet(readpath + "/dt=%s*".format(dt)).select("resource_id","html").distinct

    val articleDF = {
      val path = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=%s/hour=*"
      spark.read.parquet(path.format(dt))
        .selectExpr("resource_id as article_id","html as article", "url as article_url", "country_lang as country_lan", "category as one_level", "sub_class as two_level", "null as three_level")
        .withColumn("three_level",col("three_level").cast(StringType))
        .repartition(512)
        .dropDuplicates("article")
    }.filter("length(article) > 4500")

    val wiki = spark.read.parquet("/user/zoushuai/wiki_title").map(x => (x.getAs[String]("title"),"1")).collect().toSet
    val dict_bd = sc.broadcast(wiki)

    val
    val stop = sc.textFile()
    val wordArr = stop.collect()
    var stopwordsSet: Set[String] = Set[String]()
    for (word <- wordArr) {
      stopwordsSet += word
    }

    def get_Ngrams(split_words: Seq[String], minSize: Int, maxSize: Int): Seq[String] = {
      var ngramsList = List[List[String]]()
      var ngrams = List[String]()

      val num = split_words.size

      /* n-gram核心代码，需要保持词的顺序 */
      for (i <- 0 until num) {
        if (i+1 < num) {
          if (stopwordsindexSeq(i) == 0 && stopwordsindexSeq(i+1) == 0 ) {
            for (ngramSize <- minSize to maxSize) {
              if (i + ngramSize <= size) {
                var ngram = List[String]()
                for (j <- i until i + ngramSize) {
                  ngram = seq(j) :: ngram
                }
                ngramsList = ngram :: ngramsList
              }
            }
          } else if ((stopwordsindexSeq(i) == 0 && stopwordsindexSeq(i+1) == 1)
            || (i > 0 && stopwordsindexSeq(i-1) == 1 && stopwordsindexSeq(i) == 0 &&  stopwordsindexSeq(i+1) == 1)) {
            var ngram = List[String]()
            ngram = seq(i) :: ngram
            ngramsList = ngram :: ngramsList
          }
        } else if (i == size - 1 && stopwordsindexSeq(i) == 0 ) {
          val ngram = List[String](seq(i))
          ngramsList = ngram :: ngramsList
        }
      }
      if (ngramsList.nonEmpty) {
        for (ngram <- ngramsList) {
          ngrams = ngram.mkString(" ") :: ngrams
        }
      }
      ngrams
    }

    val art_dict_df = articleDF.sample(false, 0.1).filter("article is null").map{
      r =>
        val resource_id = r.getAs[String]("article_id")
        val text = r.getAs[String]("article")
        val split_word = {
          text.replaceAll("(\\s*\\p{P}\\s+|\\s+\\p{P}\\s*)"," ")
            .replaceAll("\\s+", " ")
            .split(" ").toSeq
        }
        val words_Ngram = get_Ngrams(split_word,0,4).toSet
        val output = dict_bd.value.toSet.intersect(words_Ngram).toSeq
        (resource_id,output)
    }.toDF("article_id","entity_keywords")
    news_clean.write.mode(SaveMode.Overwrite).save(savepath + "/dt=" + dt)
    spark.stop()
  }
}
