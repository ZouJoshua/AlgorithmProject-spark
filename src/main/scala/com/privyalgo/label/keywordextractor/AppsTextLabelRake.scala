package com.privyalgo.label.keywordextractor

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.clulab.processors.Document
import org.clulab.processors.fastnlp.FastNLPProcessor

/**
  * Created by caifuli on 17-7-10.
  */
class AppsTextLabelRake (
                          text: DataFrame,
                          bcStopWords: Broadcast[Set[String]]
                        ) extends Serializable{
  val sentenceDelimiter = """[\\[\\]\n.!?,;:\t\\-\\"\\(\\)\\\'\u2019\u2013]""".r
  val maxWordsInPhrase: Int = 2
  val minCharLength: Int = 1

  def run(): RDD[(String,List[String])] = {

    val res = text.rdd.map{r =>
      var resLabel: List[String] = List()
      var resList: List[String] = List()
      val pn = r.getAs[String]("pn")
      val realLang = r.getAs[String]("real_lang")
      val origLang = r.getAs[String]("orig_lang")
      if("en".equals(realLang) && "en".equals(origLang)){
        val desc = r.getAs[String]("desc_info").replaceAll("[a-zA-z]+:?/+[^\\p{Z}]*", "")
        val infoWords = getTextLabel(desc, bcStopWords).take(100)
        resLabel = getResultLabel(infoWords)
        for(res <- resLabel){
          if(res.split(" ").length == 1){
            resList = res :: resList
          }
        }
      }

      (pn, resList.reverse)
    }.filter(_._2.length > 0)
    res
  }


  def getTextLabel(text:String, bcStopWords: Broadcast[Set[String]]): List[(String, Double)] ={
    //先将文章分割成句子的列表,对句子中的单词进行词还原
    val proc = new FastNLPProcessor()
    val doc: Document = proc.mkDocument(text)
    proc.tagPartsOfSpeech(doc)
    proc.lemmatize(doc)
    doc.clear()

    var sentenceList:List[String] = List()
    for(sentence <- doc.sentences){
      var lemmaList: List[String] = List()
      sentence.lemmas.foreach(lemmas => {
        for (lemma <- lemmas) {
          lemmaList = lemma.toLowerCase :: lemmaList
        }
      })
      val sen = lemmaList.reverse.mkString(" ") + ". "
      sentenceList = sen :: sentenceList
    }


    //生成候选词表
    val listOfPhrases = generateCandidateKeywords(sentenceList.reverse)
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

}