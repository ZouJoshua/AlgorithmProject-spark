package com.privyalgo.label.lexicalanalyzer

import com.privyalgo.util.AlgoConstants
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.clulab.processors.Document
import org.clulab.processors.fastnlp.FastNLPProcessor


/**
  * app正文描述信息的分词程序
  * Created by caifuli on 17-8-15.
  */
class AppsTextCoreNLPTokens(
                             text: DataFrame,
                             bcStopWords: Broadcast[Set[String]],
                             tfThres: String) extends Serializable{
  def run():RDD[String] = {
    val sb: StringBuilder = new StringBuilder()

    /** 创建一下desc的RDD,为了统计词频 **/
    val descRDD = text.rdd.map { r =>
      sb.delete(0, sb.length)
      val realLang = r.getAs[String]("real_lang")
      val origLang = r.getAs[String]("orig_lang")
      if ("en".equals(realLang) && "en".equals(origLang)) {
        val desc = r.getAs[String]("desc_info")
        val cleanedDesc = cleanString(desc.replaceAll("[a-zA-z]+:?/+[^\\p{Z}]*", " "))
        sb.append(cleanedDesc)
      }
      sb.toString()
    }.filter(_.length > 0)

    val filteredWordMap = totalTFStatistic(descRDD).filter(i => i._2.toInt < tfThres.toInt).collect().toMap

    val res = text.rdd.map { r =>
      sb.delete(0, sb.length)
      val proc = new FastNLPProcessor()
      val pn = r.getAs[String]("pn")
      val realLang = r.getAs[String]("real_lang")
      val origLang = r.getAs[String]("orig_lang")
      if ("en".equals(realLang) && "en".equals(origLang)) {
        val desc = r.getAs[String]("desc_info")
        val cleanedDesc = cleanString(desc.replaceAll("[a-zA-z]+:?/+[^\\p{Z}]*", " "))
        val infoWords = getTextTokens(cleanedDesc, bcStopWords, filteredWordMap, proc)
        sb.append(pn).append(AlgoConstants.TAB_SEPERATOR).append(infoWords)
      }
      sb.toString()
    }.filter(_.length > 0)

    res
  }

  def getTextTokens(text: String, bcStopWords: Broadcast[Set[String]], filteredWordMap: Map[String, Int], proc: FastNLPProcessor): String = {
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
        if(!filteredWordMap.contains(word) && word != ""){
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
      val ngramList = getNGrams(lemmaList, 1, 2, stopwordsLabel)
      val wordsOut = new StringBuilder()
      val ngramOut = new StringBuilder()

      for (lemma <- nonstopwordList) {
      val pos = lemma2tagMap.get(lemma).last

        if (pos.contains("NN") || pos.contains("JJ") || pos.contains("VB")) {
          if (StringUtils.isNotBlank(wordsOut.toString()) || wordsOut.toString() != "") {
            wordsOut.append(AlgoConstants.BLANK_SEPERATOR)
          }
          wordsOut.append(lemma)
        }
      }

      if (ngramList.nonEmpty) {
        for (ngram <- ngramList.reverse) {
          val arr = StringUtils.split(ngram, ' ')
          val len = arr.length
          len match {
            case 2 => {
              val pos0 = lemma2tagMap.get(arr(0)).last
              val pos1 = lemma2tagMap.get(arr(1)).last
              /* 名词+名词，动词+名词，名词+名词，名词+动词，形容词+名词*/
              if ((pos0.contains("NN") && pos1.contains("NN")) || (pos0.contains("VB") && pos1.contains("NN")) || (pos0.contains("NN") && pos1.contains("VB")) || (pos0.contains("JJ") && pos1.contains("NN"))) {
                if (StringUtils.isNotBlank(ngramOut.toString())) {
                  ngramOut.append(AlgoConstants.COMMA_SEPARATOR)
                }
                ngramOut.append(ngram)
              }
            }
            case 1 => {
              val pos = lemma2tagMap.get(arr(0)).last
              /* 2017-04-24 词性过滤，只获取名词(NN)，动词(VB)，形容词(JJ) */
              if (pos.contains("NN") || pos.contains("JJ")) {
                if (StringUtils.isNotBlank(ngramOut.toString())) {
                  ngramOut.append(AlgoConstants.COMMA_SEPARATOR)
                }
                ngramOut.append(ngram)
              }
            }
          }
        }
      }

      if (StringUtils.isNotBlank(resultWord.toString()) || resultWord.toString() != "") {
        resultWord.append(AlgoConstants.BLANK_SEPERATOR)
      }
      resultWord.append(wordsOut.toString())

      if (StringUtils.isNotBlank(resultNgram.toString()) || resultNgram.toString() != " ") {
        resultNgram.append(AlgoConstants.COMMA_SEPARATOR)
      }

      resultNgram.append(ngramOut.toString())
    }

    resultWord.toString() + AlgoConstants.VERTICAL_BAR_SEPERATOR + resultNgram.toString()
  }

  def getNGrams(seq: Seq[String], minSize: Int, maxSize: Int, stopwordsindexSeq: Seq[Int]): Seq[String] = {
    var ngramsList = List[List[String]]()
    var ngrams = List[String]()

    val size = seq.size

    /* 2-gram核心代码，需要保持词的顺序 */
    for (i <- 0 until size) {
      if (i+1 < size) {
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

  def stopWordsIndex(seq: Seq[String], stopwordsSet: Set[String]): Seq[Int] = {
    val size = seq.size
    val array = new Array[Int](size)
    for (i <- 0 until size) {
      if (stopwordsSet.contains(seq(i))) {
        array(i) = 1
      }
    }
    array.toSeq
  }

  def cleanWord(word: String): String = {
    var isUnusual: Boolean = false
    var res: String = ""
    for (c <- word) {
      if (!((c >= 65 && c <= 90) || (c >= 97 && c <= 122) || c == 32)) {
        isUnusual = true
      }
    }
    if(!isUnusual){
      res = word
    }

    res
  }

  def cleanString(str: String) : String = {
    val sb: StringBuilder = new StringBuilder
    for(c <- str){
      if((c >= 65 && c <= 90) || (c >= 97 && c <= 122) || (c >= 32 && c <= 63)){
        sb.append(c)
      }
    }
    sb.toString()
  }

  def totalTFStatistic(textRDD: RDD[String]): RDD[(String, Int)] = {
    val words = textRDD.flatMap(_.split(" ")).map(word => (cleanWord(word),1))
    val wordscount = words.reduceByKey(_ + _)

    wordscount
  }

}
