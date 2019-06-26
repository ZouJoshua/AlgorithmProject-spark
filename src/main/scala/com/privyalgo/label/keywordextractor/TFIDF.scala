package com.privyalgo.label.keywordextractor

import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable
import scala.collection.mutable.WrappedArray

/**
  * Created by caifuli on 17-8-16.
  */
class TFIDF (
              tokensDF: DataFrame,
              sqlc: HiveContext,
              tfThres: String,
              idfPercent: String) extends Serializable{
  import sqlc.implicits._

  def run(): DataFrame = {
    /* TODO numFeatures默认为2的20次方 */
    val sc = sqlc.sparkContext
    val hashingTf = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(80000000)
    val featurizedData = hashingTf.transform(tokensDF)

    val total_cnt = featurizedData.count
    val bctotal_cnt = sc.broadcast(total_cnt)

    val tf1 = new org.apache.spark.mllib.feature.HashingTF(80000000)

    val pn_tfDF = featurizedData.map {
      line =>
        var wordsIndexMap: Map[String, Int] = Map()
        var tfvalueMap: Map[String, Double] = Map()
        val pagename = line.getAs[String]("pagename")
        val words = line.getAs[mutable.WrappedArray[String]]("words")
        for (word <- words) {                                      //词和索引的对应
          wordsIndexMap += (word -> tf1.indexOf(word))
        }
        val rawfeatures = line.getAs[SparseVector]("rawFeatures")
        for ((k, v) <- wordsIndexMap) {                            //词和tf的对应
          tfvalueMap += (k -> rawfeatures.apply(v))
        }

        val tfValue = tfvalueMap.toList

        (pagename,tfValue)
    }.filter(_._2.length > 0).toDF("tf_pn", "tf_value")

    val tf_text = pn_tfDF.rdd.map{r =>
      val tf = r.getAs[WrappedArray[Row]]("tf_value")
      tf.map{case Row(word: String, tf: Double) => (word, tf)}
    }.flatMap(l=>l)

    //计算total_tf并过滤
    val total_tfDF = tf_text.map(line =>{
      val word = line._1
      val tf = line._2
      (word, tf)
    }).reduceByKey((x,y) => x + y).toDF("word","total_tf")
    //total_tfDF.write.mode("overwrite").parquet("/user/caifuli/app_tags/app_tags/20170717/tmp/total_tf")

    val total_tfMap = total_tfDF.map{ case Row(word, tf_cnt) => (word -> tf_cnt)}.collect().toMap.asInstanceOf[Map[String, Double]]
    var filter_tfWordList: List[String] = List()
    for(k <- total_tfMap.keys){
      if(total_tfMap.get(k).get < tfThres.toDouble && k.split(" ").length == 1){      //TODO：tf阈值,比分词的tf阈值大1
        filter_tfWordList = k :: filter_tfWordList
      }
    }

    val filter_tfWordMap = filter_tfWordList.map(word => (word, 1.0)).toMap
    val bcfilter_tfWordMap = sc.broadcast(filter_tfWordMap)


    //计算total_file
    val total_fileDF = tf_text.map(line =>{
      val word = line._1
      val tf = 1.0
      (word,tf)
    }).reduceByKey((x,y) => x + y).toDF("word","total_file")
    //total_fileDF.write.mode("overwrite").parquet("/user/caifuli/app_tags/app_tags/20170717/tmp/total_file")

    val total_fileMap = total_fileDF.map{ case Row(word, file_cnt) => (word -> file_cnt)}.collect().toMap.asInstanceOf[Map[String, Double]]
    val bctotal_fileMap = sc.broadcast(total_fileMap)

    //计算idf
    val tfidfDF = featurizedData.map{r =>

      var wordsIndexMap: Map[String, Int] = Map()
      var tfvalueMap: Map[String, Double] = Map()
      var idfvalueMap: Map[String, Double] = Map()
      var tfidfvalueMap: Map[String, Double] = Map()

      val pagename = r.getAs[String]("pagename")
      val words = r.getAs[mutable.WrappedArray[String]]("words")

      for (word <- words) {                                      //词和索引的对应
        wordsIndexMap += (word -> tf1.indexOf(word))
      }

      val rawfeatures = r.getAs[SparseVector]("rawFeatures")
      for ((k, v) <- wordsIndexMap) {                            //词和tf的对应
        tfvalueMap += (k -> rawfeatures.apply(v))
      }

      val tfvalueList = tfvalueMap.toList

      for((k, v) <- wordsIndexMap){                              //词和idf的对应
        idfvalueMap += (k -> math.log((bctotal_cnt.value + 1.0) / (bctotal_fileMap.value.get(k).get + 1.0)))    //Option要再get一次才能拿到里边的元素～～～
      }
      val idfvalueList = idfvalueMap.toList

      for((k, v) <- wordsIndexMap){
        tfidfvalueMap += (k -> tfvalueMap.get(k).get * idfvalueMap.get(k).get)
      }
      val tfidfvalueList = tfidfvalueMap.toList

      (pagename, tfvalueList, idfvalueList, tfidfvalueList)
    }.toDF("pn", "tf_value", "idf_value", "tfidf_value")

    //计算idf阈值
    val idfThreshold = calcIDFThreshold(tfidfDF, sqlc, idfPercent)

    /* 对tf*idf值进行排序，取经过idf阈值筛选后的值 */
    val resDF = tfidfDF.map { r =>
      val pagename = r.getAs[String]("pn")
      val idf = r.getAs[WrappedArray[Row]]("idf_value")
      val tfidf = r.getAs[WrappedArray[Row]]("tfidf_value")
      var filter_idfWordMap: Map[String, Double] = Map()
      var filtered_idfWordList: List[String] = List()

      //按idf阈值筛选
      for(word_idf <- idf){
        val idfValue = word_idf.get(1).asInstanceOf[Double]    //取出idf值
        if(idfValue < idfThreshold){
          filtered_idfWordList = word_idf.get(0).asInstanceOf[String] :: filtered_idfWordList
        }
      }

      //对tfidf值进行降序排序，组成最后的标签
      var resList: List[(String, Double)] = List()     //去掉经idf阈值和tf值筛选之后的词表
    val tfidfList = tfidf.map{case Row(word: String, tfidf: Double) => (word, tfidf)}.toList
      filter_idfWordMap = filtered_idfWordList.map(word => (word, 1.0)).toMap

      for(word <- tfidfList){
        if(!filter_idfWordMap.contains(word._1) && !bcfilter_tfWordMap.value.contains(word._1) ){
          resList = word :: resList
        }
      }

      val sortedresList = resList.sortBy(_._2)
      var list: List[String] = List()
      for(res <- sortedresList){
        list = res._1 :: list
      }

      (pagename, list)
    }.filter(_._2.length > 0).toDF("tf_pn", "tf_tags")

    resDF
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

  /**按idf进行过滤**/
  def calcIDFThreshold(idfDF: DataFrame, sqlc: HiveContext, idfPercent: String): Double = {
    val idfPer = idfPercent.toDouble
    var idfThres: Double = 0.0
    val idf_text = idfDF.rdd.map{r =>
      val idf = r.getAs[WrappedArray[Row]]("idf_value")
      idf.map{case Row(word: String, idf: Double) => (word, idf)}
    }.flatMap(l=>l)

    val residfDF = idf_text.toDF("word","idf")
    residfDF.registerTempTable("idfTable")

    idfThres = sqlc.sql(s"select Percentile_approx(idf, $idfPer) as idfper from idfTable").first().getAs[Double]("idfper")      //todo:idf百分比阈值

    idfThres
  }

}
