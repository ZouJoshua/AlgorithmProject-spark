package com.privyalgo.label.keywordextractor

import com.privyalgo.util.AlgoConstants
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable

/**
  * Created by caifuli on 17-8-16.
  */
class LabelCombine (trDF: DataFrame,
                    rakeDF: DataFrame,
                    tfDF: DataFrame,
                    sqlc: SQLContext) extends Serializable{
  import sqlc.implicits._

  def run(): DataFrame = {
    val uf = udf((a: String, b: String) => {
      var res = ""
      if (null != a)
        res = a
      else if (null == a && null != b)
        res = b
      res
    })

    /** three DataFrame join **/
    val outerJoinDF = trDF.join(rakeDF, trDF.col("tr_pn") === rakeDF.col("rake_pn"), "outer")     //tr和rake进行结合
    val tempJoinDF = outerJoinDF.withColumn("tmp_pn", uf(outerJoinDF("tr_pn"), outerJoinDF("rake_pn")))
    val wholeJoinDF = tempJoinDF.join(tfDF, tfDF.col("tf_pn") === tempJoinDF.col("tmp_pn"),"outer")  //三个df结合
    val joinDF = wholeJoinDF.withColumn("pn", uf(wholeJoinDF("tf_pn"), wholeJoinDF("tmp_pn")))
    val tempDF1 = joinDF.drop("tf_pn")
    val tempDF2 = tempDF1.drop("rake_pn")
    val tempDF3 = tempDF2.drop("tmp_pn")
    val resDF = tempDF3.drop("tr_pn")

    val df = resDF.map(r => {
      val pagename = r.getAs[String]("pn")
      var resList: List[String] = List()
      var phraseList: List[String] = List()
      var wordList: List[String] = List()
      var resCombineList: List[String] = List()
      try {
        val tfLabel = r.getAs[mutable.WrappedArray[String]]("tf_tags")
        val trLabel = r.getAs[mutable.WrappedArray[String]]("tr_label")
        val rakeLabel = r.getAs[mutable.WrappedArray[String]]("rake_tags")
        resCombineList = (trLabel ++ rakeLabel ++ tfLabel).toList

        var wordsList: List[String] = List()
        for(phrase <- resCombineList){
          val wordList = phrase.split(AlgoConstants.BLANK_SEPERATOR)
          for(word <- wordList){
            wordsList = word.trim() :: wordsList
          }
        }

        /** 计算各个词的词频 **/
        val wordLabelWithOne = wordsList.map(word =>(word,1))

        val seqop = (result: mutable.HashMap[String, Int], wordcount: (String, Int)) => {
          val addOne = (wordcount._1, result.getOrElse(wordcount._1, 0) + wordcount._2)
          result.+=(addOne)
        }
        val combop = (result1: mutable.HashMap[String, Int], result2: mutable.HashMap[String, Int]) => {
          result1 ++= result2
        }
        val wordcount = wordLabelWithOne.aggregate(mutable.HashMap[String, Int]())(seqop,combop)

        /** 计算结果中各个词的tf值之积 **/
        var phraseValueList: List[(String, Int)] = List()
        for(phrase <- resCombineList){
          val wordList = phrase.split(AlgoConstants.BLANK_SEPERATOR)
          val countProductValue = wordList.map(word => wordcount.get(word).get).product
          val phraseValue =  (phrase, countProductValue)
          phraseValueList = phraseValue :: phraseValueList
        }

        /**对map中的Value值进行降序排序**/
        val sortedPhraseValueList = phraseValueList.sortWith(_._2 > _._2)
        for(sortedPhrase <- sortedPhraseValueList){
          val word = sortedPhrase._1
          val len = word.split(AlgoConstants.BLANK_SEPERATOR).length
          if(!phraseList.contains(word) && len == 2){
            phraseList = word :: phraseList
          }
        }

        for(word <- tfLabel){
          val len = word.split(AlgoConstants.BLANK_SEPERATOR).length
          if(!wordList.contains(word) && len == 1 && word != ""){
            wordList = word :: wordList
          }
        }

        resList = wordList.reverse.take(15) ++ phraseList.reverse.take(15)

      } catch {
        case _: Exception =>
      }

      (pagename, resList)
    }).filter(_._2.size > 0).toDF("pn", "tag")

    df
  }

}
