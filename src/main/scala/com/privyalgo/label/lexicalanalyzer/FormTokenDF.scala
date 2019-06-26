package com.privyalgo.label.lexicalanalyzer

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by caifuli on 17-8-15.
  */
class FormTokenDF (
                    text: RDD[String],
                    sqlc:SQLContext
                  ) extends Serializable{
  import sqlc.implicits._
  def run(): DataFrame = {
    val wordDF = text.map(line => {  //wordDF是把所有|前，|后的词都汇总起来，整合成一个dataframe
    val lineArr = StringUtils.split(line, '\t')
      var pagename:String = null
      var list = List[String]()
      var ngramList = List[String]()
      try{
        pagename = lineArr(0)    //包名：a.b.drunkenfriend
        val words = lineArr(1)     //对应的textlabel：app delete send receive sm drunk choice delete sm wake drunk night start app day get choise delete sm keep record decide start end session perfect match drunken night|app,app delete,delete,send,receive,receive sm,sm,drunk,c

        var wordStr = ""
        var ngramWords = ""
        try {
          val wordArr = StringUtils.split(words, '|')
          wordStr = wordArr(0)   //单个词
          ngramWords = wordArr(1)    //含组合词的
        } catch {
          case _: Exception =>
        }
        if (StringUtils.isNotBlank(wordStr) && StringUtils.isNotBlank(ngramWords)) {
          val wordArr = StringUtils.split(wordStr, ' ')
          for (word <- wordArr) {
            val strByte = word.map(_.toByte)
            if(!judgeUnusualChar(strByte)) {
              list = word :: list
            }
          }
          val ngramArr = StringUtils.split(ngramWords, ',')
          for (ngram <- ngramArr) {
            val strByte = ngram.map(_.toByte)
            if (!judgeUnusualChar(strByte)) {
              ngramList = ngram :: ngramList
            }
          }
        }
      }catch{
        case _: Exception =>
      }

      (pagename, list, ngramList)
    }).filter(_._2.length > 0).toDF("pagename","words", "ngrams")

    wordDF
  }

  def judgeUnusualChar(arr: Seq[Byte]): Boolean = {
    var isUnusal = false

    for (a <- arr) {
      if (!((a >= 65 && a <= 90) || (a >= 97 && a <= 122) || a == 32)) {
        isUnusal = true
      }
    }

    isUnusal
  }

}
