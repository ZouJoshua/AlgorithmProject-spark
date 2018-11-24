package com.apus.nlp

import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.clulab.processors.Document
import org.clulab.processors.fastnlp.FastNLPProcessor

/**
  * Created by Joshua on 2018-11-20
  */
object PrepProcess {

  /**
    *处理较干净的正文内容
    * 1.二次清洗（清洗标点、大小写等）
    * 2.分词（ngrams）
    * 3.找wiki关联实体词
    */

  def cleanString(str: String): String = {
    /**
      * 二次清洗
      */
    if (str == null || str == "" || str.length <= 0 || str.replaceAll(" ", "").length <= 0 || str == "None") {
      null  //wiki词库存在None单词，清洗后转化为null
    } else {
      str.toLowerCase.trim().replaceAll("[\r\n\t]", " ")
        .replaceAll("(&amp;|&#13;|&nbsp;)","")
        .replaceAll("[ ]+"," ")
        .replaceAll("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]","")
        .replaceAll("^(\\w+((-\\w+)|(\\.\\w+))*)\\+\\w+((-\\w+)|(\\.\\w+))*\\@[A-Za-z0-9]+((\\.|-)[A-Za-z0-9]+)*\\.[A-Za-z0-9]+$","")
    }
  }

  def get_Ngrams(words:Seq[String], minSize: Int = 1, maxSize: Int = 4):Seq[String] ={
    /**
      * 切词，转化为 ngrams
      */
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
    val savepath = "/user/zoushuai/news_content/article_ngrams/dt=%s"

    // 读取较干净的新闻正文内容
    val articleDF = {
      spark.read.parquet(newspath.format(dt))
        .selectExpr("resource_id as article_id", "html", "article")
//        .withColumn("three_level",col("three_level").cast(StringType))
        .repartition(512)
        .dropDuplicates("article")
    }.filter("length(article) > 100").limit(10000)
    articleDF.cache

    // 读取wiki词库
//    val wiki = spark.read.parquet(wikipath).map(r => cleanString(r.getAs[String]("title"))).collect().filter(_!=null)
//    val wiki_bd = sc.broadcast(wiki)
    // 用Map结果contains wiki词库，速度更快
    val wiki_map = spark.read.parquet(wikipath).map(x => (cleanString(x.getAs[String]("title")),1)).collect().toMap
    val wikiWordMap = sc.broadcast(wiki_map)

    // 直接从文章正文查找wiki词条
    val art_wiki_df = articleDF.rdd.map{
      r =>
        val id = r.getAs[String]("article_id").toString
        val html= cleanString(r.getAs[String]("article"))
        val split_word = {
          html.replaceAll("(\\s*\\p{P}\\s+|\\s+\\p{P}\\s*)"," ")
            .replaceAll("\\s+", " ")
            .split(" ").toSeq
        }
        val wordsNgram = get_Ngrams(split_word, 1, 4).toSet
        var hit_dict: List[String] = List()
//        var hit_dict = Seq.empty[String]
        for(word <- wordsNgram){
          if(!wikiWordMap.value.contains(word) && word != ""){
            hit_dict = word :: hit_dict
          }}
        (id,hit_dict)
    }.toDF("resource_id", "entity_keywords")


//    //Todo：分词后（4元词组）与wiki词库匹配，可能存在部分短语匹配不上
//    val art_wiki_df = articleDF.filter("article is not null").map{
//      r =>
//        val id = r.getAs[String]("article_id")
//        val text = cleanString(r.getAs[String]("article"))
//        val split_word = {
//          text.replaceAll("(\\s*\\p{P}\\s+|\\s+\\p{P}\\s*)"," ")
//            .replaceAll("\\s+", " ")
//            .split(" ").toSeq
//        }
//        val wordsNgram = get_Ngrams(split_word, 1, 4).toSet
//        val output = wiki_bd.value.toSet.intersect(wordsNgram).toSeq
//        (id,output)
//    }.toDF("article_id","entity_keywords")

    // 处理停用词
//    val file = "/user/caifuli/new_stopwords.txt"
    def get_Stopwords(file:String="/user/caifuli/new_stopwords.txt"): Set[String] ={
      val stop = sc.textFile(file)
      val wordArr = stop.collect()
      var stopwordsSet: Set[String] = Set[String]()
      for (word <- wordArr) {
        stopwordsSet += word
      }
      stopwordsSet
    }
    val stopwords_bd = sc.broadcast(get_Stopwords())

    // 进行ngrams分词
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
        (id,ngrams)
    }.toDF("article_id","n_grams")
    Ngrams_df.cache

    // 进行ngrams分词（词性标注

    def getTextTokens(text: String, bcStopWords: Broadcast[Set[String]], wikiWordMap: Broadcast[Map[String, Int]], proc: FastNLPProcessor): String = {
      val resultWord = new StringBuilder()
      val resultNgram = new StringBuilder()

      /**对文本进行预处理,删掉低频词，这是因为服务端那边的坑**/
      val sentenceDelimiter = """[\\[\\]\n.!?,;:\t\\-\\"\\(\\)\\\'\u2019\u2013]""".r
      val filtered_text: StringBuilder = new StringBuilder()
      val sentenceList = sentenceDelimiter.split(text).toList
      for(sentence <- sentenceList){
        var filtered_wordList: List[String] = List()
        val wordList = sentence.trim.split("\\s+").toList
        for(word <- wordList){
          if(!wikiWordMap.value.contains(word) && word != ""){
            filtered_wordList = word :: filtered_wordList
          }
        }

        if(filtered_wordList.nonEmpty){
          filtered_text.append(filtered_wordList.reverse.mkString(" ").trim).append(". ")
        }

      }

      val doc: Document = proc.mkDocument(filtered_text.toString())
      proc.tagPartsOfSpeech(doc)
      proc.lemmatize(doc)
      doc.clear()

      /* 将一篇文章划分为很多句子，一句一句的处理 */
      for (sentence <- doc.sentences) {
        //TODO：NER识别

        /** 词性标注 **/ //TODO：词性标注有误，不知道该怎么解决,倒是可以扩写句子
        var tagsList: List[String] = List()
        sentence.tags.foreach(tags => {
          for (tag <- tags) {
            tagsList = tag :: tagsList
          }
        })

        /** 词性还原、归一 **/
        var lemmaList: List[String] = List()
        sentence.lemmas.foreach(lemmas => {
          for (lemma <- lemmas) {
            lemmaList = lemma.toLowerCase :: lemmaList
          }
        })

        /** 根据词性来过滤掉组合词 **/
        val lemma2tag = lemmaList zip tagsList
        val lemma2tagMap = lemma2tag.toMap

        /** 判断词是否为停用词，且对停词打上标记，返回标记数组 **/
        val stopwordsLabel = stopWordsIndex(lemmaList, bcStopWords.value)
        /* 注意：需要保持词的原来顺序 */
        val lemma2index = lemmaList zip stopwordsLabel
        var nonstopwordList = List[String]()
        lemma2index.foreach( t => {
          if (t._2 == 0) {
            nonstopwordList = t._1 :: nonstopwordList
          }
        })

        /** ngram组合词 **/
        val ngramList = get_Ngrams(lemmaList, 1, 4)
        val wordsOut = new StringBuilder()
        val ngramOut = new StringBuilder()

        for (lemma <- nonstopwordList) {
          val pos = lemma2tagMap.get(lemma).last

          if (pos.contains("NN") || pos.contains("JJ") || pos.contains("VB")) {
            if (StringUtils.isNotBlank(wordsOut.toString()) || wordsOut.toString() != "") {
              wordsOut.append(" ")
            }
            wordsOut.append(lemma)
          }
        }

        if (ngramList.nonEmpty) {
          for (ngram <- ngramList.reverse) {
            val arr = StringUtils.split(ngram, ' ')
            val len = arr.length
            len match {
              case 4 => {
                val pos0 = lemma2tagMap.get(arr(0)).last
                val pos1 = lemma2tagMap.get(arr(1)).last
                /* 名词+名词，动词+名词，名词+名词，名词+动词，形容词+名词*/
                if ((pos0.contains("NN") && pos1.contains("NN")) || (pos0.contains("VB") && pos1.contains("NN")) || (pos0.contains("NN") && pos1.contains("VB")) || (pos0.contains("JJ") && pos1.contains("NN"))) {
                  if (StringUtils.isNotBlank(ngramOut.toString())) {
                    ngramOut.append(",")
                  }
                  ngramOut.append(ngram)
                }
              }
              case 3 => {
                val pos0 = lemma2tagMap.get(arr(0)).last
                val pos1 = lemma2tagMap.get(arr(1)).last
                /* 名词+名词，动词+名词，名词+名词，名词+动词，形容词+名词*/
                if ((pos0.contains("NN") && pos1.contains("NN")) || (pos0.contains("VB") && pos1.contains("NN")) || (pos0.contains("NN") && pos1.contains("VB")) || (pos0.contains("JJ") && pos1.contains("NN"))) {
                  if (StringUtils.isNotBlank(ngramOut.toString())) {
                    ngramOut.append(",")
                  }
                  ngramOut.append(ngram)
                }
              }
              case 2 => {
                val pos0 = lemma2tagMap.get(arr(0)).last
                val pos1 = lemma2tagMap.get(arr(1)).last
                /* 名词+名词，动词+名词，名词+名词，名词+动词，形容词+名词*/
                if ((pos0.contains("NN") && pos1.contains("NN")) || (pos0.contains("VB") && pos1.contains("NN")) || (pos0.contains("NN") && pos1.contains("VB")) || (pos0.contains("JJ") && pos1.contains("NN"))) {
                  if (StringUtils.isNotBlank(ngramOut.toString())) {
                    ngramOut.append(",")
                  }
                  ngramOut.append(ngram)
                }
              }
              case 1 => {
                val pos = lemma2tagMap.get(arr(0)).last
                /* 2017-04-24 词性过滤，只获取名词(NN)，动词(VB)，形容词(JJ) */
                if (pos.contains("NN") || pos.contains("JJ")) {
                  if (StringUtils.isNotBlank(ngramOut.toString())) {
                    ngramOut.append(",")
                  }
                  ngramOut.append(ngram)
                }
              }
            }
          }
        }

        if (StringUtils.isNotBlank(resultWord.toString()) || resultWord.toString() != "") {
          resultWord.append(" ")
        }
        resultWord.append(wordsOut.toString())

        if (StringUtils.isNotBlank(resultNgram.toString()) || resultNgram.toString() != " ") {
          resultNgram.append(",")
        }

        resultNgram.append(ngramOut.toString())
      }

      resultWord.toString() + AlgoConstants.VERTICAL_BAR_SEPERATOR + resultNgram.toString()
    }

    // html加标签 <i class="apus-entity-words">xxx</i>
    val tagUDF = udf{
      (article:String, words:Seq[String]) =>
        var tmp_art = " " + article + " "
        words.foreach{
          w =>
            tmp_art = tmp_art.replaceAll("\\s(?i)" + w + "(?=\\s)", " <i class=\"apus-entity-words\">" + w + "</i> ")
        }
        tmp_art
    }

    // 存储结果
    val news_result = {
//      val seqUDF =udf((t:String) => Seq.empty[String])
      articleDF.join(art_wiki_df, Seq("article_id"))
        .join(Ngrams_df, Seq("article_id"))
        .withColumn("html_tag",tagUDF(col("html"), col("entity_keywords")))
    }
    news_result.write.mode(SaveMode.Overwrite).save(savepath.format(dt))
    spark.stop()
  }
}
