package com.privyalgo.label.lexicalanalyzer

import com.alibaba.fastjson.JSON
import com.privyalgo.util.AlgoConstants
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.clulab.processors.Document
import org.clulab.processors.fastnlp.FastNLPProcessor

import scala.util.control.Breaks

/**
  * Created by caifuli on 17-6-2.
  */
class URLTextCoreNLPTokens (
                             text: RDD[String],
                             bcStopWords: Broadcast[Set[String]],
                             tfThres: String
                           ) extends Serializable{
  def run(): RDD[String] = {
    val sb: StringBuilder = new StringBuilder()

    val sentenceRDD = text.mapPartitions(p => {
      p.map(str => {
        sb.delete(0, sb.length)

        val strJson = JSON.parseObject(str)
        val titleStr = strJson.get("title").toString
        val keywordStr = strJson.get("keyword").toString
        val descStr = strJson.get("description").toString

        var origLang = ""
        // 利用原始语言和真实语言来判断
        var realLang = ""
        var count = 0
        if ("\n" == titleStr) {
          count += 1
        }
        if ("\n" == descStr) {
          count += 1
        }
        if ("\n" == keywordStr) {
          count += 1
        }

        if (count != 3) {
          origLang = strJson.get("orig_lang").toString
          realLang = strJson.get("real_lang").toString
        }

        if ("en".equals(origLang) && "en".equals(realLang)) {
          //把三条string拼一起
          val title = cleanString(titleStr.replaceAll("[a-zA-z]+:?/+[^\\p{Z}]*", "").replaceAll("\\n", "").replaceAll("\\|", "").trim)
          val keyword = cleanString(keywordStr.replaceAll("[a-zA-z]+:?/+[^\\p{Z}]*", "").replaceAll("\\n", "").replaceAll("\\|", "").trim)
          val desc = cleanString(descStr.replaceAll("[a-zA-z]+:?/+[^\\p{Z}]*", "").replaceAll("\\n", "").replaceAll("\\|", "").trim)

          if (title != "") {
            sb.append(title).append(AlgoConstants.COMMA_SEPARATOR)
          }
          if (keyword != "") {
            sb.append(keyword).append(AlgoConstants.COMMA_SEPARATOR)
          }
          if (desc != "") {
            sb.append(desc)
          }
        }
        sb.toString()
      }).filter(_.length > 0)
    })


    val filteredWordMap = totalTFStatistic(sentenceRDD).filter(i => i._1 != "").filter(i => i._2.toInt < tfThres.toInt).collect().toMap     //todo:tf阈值tfThres


    val res = text.mapPartitions( p => {
      val proc = new FastNLPProcessor()

      p.map(str => {
        sb.delete(0, sb.length) //初始化，清空字符串

        val strJson = JSON.parseObject(str)
        val titleStr = strJson.get("title").toString
        val keywordStr = strJson.get("keyword").toString
        val descStr = strJson.get("description").toString

        var origLang = "" // 利用原始语言和真实语言来判断
        var realLang = ""
        var count = 0
        if ("\n" == titleStr){
          count += 1
        }
        if("\n" == descStr){
          count += 1
        }
        if("\n" == keywordStr){
          count += 1
        }

        if (count != 3) {
          origLang = strJson.get("orig_lang").toString
          realLang = strJson.get("real_lang").toString
        }
        if ("en".equals(origLang) && "en".equals(realLang)) {
          val sentenceSB: StringBuilder = new StringBuilder()

          val title = cleanString(titleStr.replaceAll("[a-zA-z]+:?/+[^\\p{Z}]*", "").replaceAll("\\n","").replaceAll("\\|","").trim)
          val keyword = cleanString(keywordStr.replaceAll("[a-zA-z]+:?/+[^\\p{Z}]*", "").replaceAll("\\n","").replaceAll("\\|","").trim)
          val desc = cleanString(descStr.replaceAll("[a-zA-z]+:?/+[^\\p{Z}]*", "").replaceAll("\\n","").replaceAll("\\|","").trim)

          if(title != ""){
            sentenceSB.append(title).append(AlgoConstants.COMMA_SEPARATOR)
          }
          if(keyword != ""){
            sentenceSB.append(keyword).append(AlgoConstants.COMMA_SEPARATOR)
          }
          if(desc != ""){
            sentenceSB.append(desc)
          }

          val sentence = sentenceSB.toString()
          val infoWords = getTextTokens(sentence, bcStopWords, filteredWordMap, proc)
          sb.append(strJson.get("url").toString).append(AlgoConstants.TAB_SEPERATOR).append(infoWords)
        }
        sb.toString()
      }).filter(_.length > 0)
    })

    res
  }

  /**获取文本标签
    * text:一条清洗过的desc_info
    */
  def getTextTokens(text: String, bcStopWords: Broadcast[Set[String]], filteredWordMap: Map[String, Int], proc: FastNLPProcessor): String = {
    val resultWord = new StringBuilder()
    val resultNgram = new StringBuilder()
    var res: List[String] = List()

    /**对文本进行预处理**/
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

    /* 将一篇文章划分为很多句子，一句一句的处理,以.作为分割， */
    for (sentence <- doc.sentences) {

      /**找出所有潜在NER,为TFIDF服务**/
      var potentialNER: List[String] = List()
      sentence.words.foreach(word =>{
        if(judgeNERWord(word)){
          if(!bcStopWords.value.contains(word.toLowerCase()) && word.length < 20){    //todo：单词长度阈值，不知道这个值是否要改
            potentialNER = word :: potentialNER
          }
        }
      })

      /**词性标注**/
      var tagsList: List[String] = List()
      sentence.tags.foreach(tags => {
        for (tag <- tags) {
          tagsList = tag :: tagsList
        }
      })

      /**词性还原、归一**/
      var lemmaList: List[String] = List()
      sentence.lemmas.foreach(lemmas => {
        for (lemma <- lemmas) {
          lemmaList = lemma.toLowerCase :: lemmaList
        }
      })

      /**根据词性来过滤掉组合词 **/
      val lemma2tag = lemmaList zip tagsList
      val lemma2tagMap = lemma2tag.toMap

      /**判断词是否为停用词，且对停词打上标记，返回标记数组**/
      val stopwordsLabel = stopWordsIndex(lemmaList, bcStopWords.value)

      /* 注意：需要保持词的原来顺序 */
      val lemma2index = lemmaList zip stopwordsLabel
      var wordList = List[String]()
      lemma2index.foreach( t => {
        if (t._2 == 0) { // not stopwords
          wordList = t._1 :: wordList
        }
      })

      /**ngram组合词**/
      val ngramList = getNGrams(lemmaList, 1, 2, stopwordsLabel)

      val wordsOut = new StringBuilder()
      val ngramOut = new StringBuilder()

      /**把名词、动词、形容词放到workOut里，词与词之间用空格分开**/
      for (lemma <- wordList) {
      val pos = lemma2tagMap.get(lemma).last
        if (pos.contains("NN") || pos.contains("JJ") || pos.contains("VB")) {
          if (StringUtils.isNotBlank(wordsOut.toString())) {
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
              /* 名词+名词，动词+名词，名词+动词，形容词+名词*/
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
              if (pos.contains("NN") || pos.contains("JJ")/* || pos.contains("VB")*/) {
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

      var finalNERList: List[String] = List()                         //去掉已在组合词列表中的部分
      for(word <- potentialNER){
        val ngramOutList = ngramOut.toString().split(",")
        if(!ngramOutList.contains(word.toLowerCase) && word != ""){
          finalNERList = word :: finalNERList
        }
      }

      if(finalNERList.nonEmpty){
        resultNgram.append(ngramOut.toString()).append(AlgoConstants.COMMA_SEPARATOR).append(finalNERList.mkString(","))
      }else{
        resultNgram.append(ngramOut.toString())
      }
    }

    resultWord.toString() + AlgoConstants.VERTICAL_BAR_SEPERATOR + resultNgram.toString()
  }

  /** 获得组合词，设定组合词的最长和最短词个数**/
  def getNGrams(seq: Seq[String], minSize: Int, maxSize: Int, indexSeq: Seq[Int]): Seq[String] = {
    var ngramsList = List[List[String]]()
    var ngrams = List[String]()

    val size = seq.size

    /* 2-gram核心代码，需要保持词的顺序 */
    for (i <- 0 until size) {
      if (i+1 < size) {
        if (indexSeq(i) == 0 && indexSeq(i+1) == 0) {
          for (ngramSize <- minSize to maxSize) {
            if (i + ngramSize <= size) {
              var ngram = List[String]()
              for (j <- i until i + ngramSize) {
                ngram = seq(j) :: ngram
              }
              ngramsList = ngram :: ngramsList
            }
          }
        } else if ((indexSeq(i) == 0 && indexSeq(i+1) == 1) || (i > 0 && indexSeq(i-1) == 1 && indexSeq(i) == 0 &&  indexSeq(i+1) == 1)) {
          var ngram = List[String]()
          ngram = seq(i) :: ngram
          ngramsList = ngram :: ngramsList
        }
      } else if (i == size - 1 && indexSeq(i) == 0) {
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

  def judgeNERWord(word:String): Boolean = {
    var isNER = false
    var isCapital = false          //单词首字母是否大写
    var isContinuousCapital = false     //单词是否为连续大写  eg.COURSEThis
    var isUnusual = false
    var count:Int = 0              //单词中大写字符数

    if (word(0) >= 65 && word(0) <= 90) {   //65-90 : A-Z
      isCapital = true
    }
    for(char <- word){
      if(char >= 65 && char <=90){
        count += 1
      }
    }
    if(count == word.length){
      isContinuousCapital = true
    }
    val loop = new Breaks
    loop.breakable{
      for(a <- word){
        if(!((a >= 65 && a <= 90) || (a >= 97 && a <= 122) || a == 32)){
          isUnusual = true
          loop.break()
        }
      }
    }
    if((isContinuousCapital || (isCapital && count == 1 )) && !isUnusual){
      isNER = true
    }

    isNER
  }

  /**对出现停用词的单个词用值1代替，返回的是0/1的列表**/
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

  def totalTFStatistic(textRDD: RDD[String]): RDD[(String, Int)] = {

    val words = textRDD.flatMap(_.split(" ")).map(word => cleanWord(word)).filter(word => word !="")
    val pairs = words.map(word => (word, 1))
    val wordscount = pairs.reduceByKey(_ + _)

    wordscount
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

}
