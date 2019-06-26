package com.privyalgo.label.keywordextractor

import com.alibaba.fastjson.JSON
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by caifuli on 17-7-25.
  */
class SearchwordLabelRake(
                           text: RDD[String],
                           bcStopWords: Broadcast[Set[String]]
                         ) extends Serializable {
  val sentenceDelimiter = """[\\[\\]\n.!?,;:\t\\-\\"\\(\\)\\\'\u2019\u2013]""".r
  val maxWordsInPhrase: Int = 2
  val minCharLength: Int = 1

  def run(): RDD[(String,List[String])] = {

    val res = text.map(line => {
      val strJson = JSON.parseObject(line)
      val searchwords = strJson.get("searchword").toString
      var resLabel: List[String] = List()

      if(strJson.containsKey("real_lang")){
        val realLang = strJson.get("real_lang").toString
        if("en".equals(realLang)){
          val searchwordInfo = cleanString(searchwords.replaceAll("[a-zA-z]+:?/+[^\\p{Z}]*", ""))
          val infoWords = getTextLabel(searchwordInfo, bcStopWords).take(100)
          resLabel = getResultLabel(infoWords)
        }
      }

      (searchwords, resLabel)
    }).filter(_._2.length > 0)
    res
  }


  def getTextLabel(text:String, bcStopWords: Broadcast[Set[String]]): List[(String, Double)] ={
    //先将文章分割成句子的列表
    val sentenceList = splitTextToSentences(text)
    //生成候选词表
    val listOfPhrases = generateCandidateKeywords(sentenceList)
    //计算单个单词得分
    val wordScores = calcWordScores(listOfPhrases)
    //计算短语得分
    val phraseScores = calcScoresForPhrase(listOfPhrases, wordScores)
    val orderedPhrases = phraseScores.toList.sortBy(x => x._2).reverse
    orderedPhrases
  }

  def splitTextToSentences(text:String): List[String] = {
    sentenceDelimiter.split(text).toList
  }

  def generateCandidateKeywords(sentences:List[String]): List[List[String]] = {
    val splittedSentences: List[List[String]] = sentences.map(sentence => sentence.trim.split("\\s+").map(_.toLowerCase).toList)    //按空白字符进行分割
    splittedSentences.flatMap(sentenceList => getPhrasesForSentence(sentenceList)(isAcceptableString))
  }

  def getPhrasesForSentence(listOfWords: List[String])(predicate: String => Boolean): List[List[String]] = {
    listOfWords match {
      case Nil => Nil
      case x :: xs =>
        val phrase = listOfWords takeWhile predicate
        if(phrase.isEmpty || phrase.length > maxWordsInPhrase){
          getPhrasesForSentence(listOfWords.drop(1))(predicate)
        }
        else{
          phrase :: getPhrasesForSentence(listOfWords.drop(phrase.length + 1))(predicate)
        }
    }
  }

  def isAcceptableString(word: String): Boolean = {
    if(word.length < minCharLength) return false
    if(!isAplha(word)) return false
    if(bcStopWords.value.contains(word)) return false
    return true
  }

  def isAplha(word: String): Boolean = {    //防止脏词出现
    word.matches("[a-z]+")
  }

  def calcWordScores(listOfPhrases: List[List[String]]): Map[String, Double] = {
    val completeListOfWords = listOfPhrases.flatten
    val wordFreqList: List[(String, Int)] = completeListOfWords.map(word => (word, 1))
    val wordDegreeListPartial: List[(String, Int)] = listOfPhrases.flatMap{ phrase =>
      val degree = phrase.length - 1
      phrase.map(word => (word, degree))
    }
    //计算freq(word)
    val wordFreqMap: Map[String, Int] = wordFreqList.groupBy(k => k._1).mapValues(l => l.map(_._2).sum)
    //计算deg(word)
    val wordDegreeMap: Map[String, Int] = (wordFreqList ++ wordDegreeListPartial).groupBy(k => k._1).mapValues(l => l.map(_._2).sum)
    //最终得分 = deg(word) / freq(word)
    wordFreqMap.keys.map(word => word -> wordDegreeMap(word).toDouble / wordFreqMap(word)).toMap
  }

  def calcScoresForPhrase(listOfPhrases: List[List[String]], wordScores: Map[String, Double]) = {
    listOfPhrases.map(phrase => phrase.mkString(" ") -> phrase.map(wordScores(_)).sum).toMap
  }

  def getResultLabel(scoreList: List[(String, Double)]): List[String] = {
    var wordList: List[String] = List()
    for(score <- scoreList){
      val word = score._1
      wordList = word :: wordList
    }
    val resultLabel = wordList.reverse
    resultLabel
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
