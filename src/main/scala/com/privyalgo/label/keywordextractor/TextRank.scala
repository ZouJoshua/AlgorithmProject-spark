package com.privyalgo.label.keywordextractor

import com.privyalgo.util.AlgoConstants
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable

/**
  * Created by caifuli on 17-8-16.
  */
class TextRank (
               tokensDF: DataFrame,
               sqlc: SQLContext
               )extends Serializable{
  import sqlc.implicits._
  def run(): DataFrame = {
    val sc = sqlc.sparkContext
    val inputRdd = tokensDF.map(r => {  //所有words列取出来组成inputRdd
      r.getAs[mutable.WrappedArray[String]]("words").toArray  //把words列取出来，生成Array
    })

    val wordRdd = inputRdd.flatMap(l => l).distinct.rdd.zipWithIndex()  //为wordDF中所有单个词自动创建一个计数器进行标号，从0开始
    val word = wordRdd.toDF("stem","id")

    val wordMap = wordRdd.collect().toMap  //对上边得出的wordDF做成一个字典
    //Map(snow -> 40, sens -> 83, health -> 5, savings -> 9, picker -> 68, application -> 37, please -> 70, interesting -> 101, finger -> 3, network -> 35, produce -> 120, method -> 160, flicker -> 31, website -> 90, random -> 10

    word.registerTempTable("word")

    val bcWordMap = sc.broadcast(wordMap)
    val edgeRdd = inputRdd.flatMap(text => getEdgesForText(text, bcWordMap, 2))
    val edge = edgeRdd.toDF("node1", "node2")
    edge.registerTempTable("edge")

    val n = sqlc.sql("SELECT id, stem FROM word").distinct().rdd
    val nodes: RDD[(Long, String)] = n.map(p => (p(0).asInstanceOf[Long], p(1).asInstanceOf[String]))  //原来是stem,id结构,现在成id,stem结构，生成一个RDD

    val e = sqlc.sql("SELECT * FROM edge").rdd
    val edges: RDD[Edge[Int]] = e.map(p => Edge(p(0).asInstanceOf[Long], p(1).asInstanceOf[Long], 0))   //生成一个新edge的RDD
    val g: Graph[String, Int] = Graph(nodes, edges)    //根据节点和边的关系构成一个图
    val r = g.pageRank(0.0001).vertices   //传入的这个参数的值越小PageRank计算的值就越精确，如果数据量特别大而传入的参数值又特别小的情况下就会导致巨大的计算任务和计算时间

    val min_rank = median(r.map(_._2).collect())   //0.712011308201017
    val filterRdd = r.map(v => (v._1.toLong, v._2)).filter(_._2 >= min_rank)  //过滤掉小于min_rank的节点
    val joinFilterRdd = filterRdd.map(v => (v._1.toLong, v._2)).join(nodes)
    val joinFilterMap = joinFilterRdd.collect().toMap
    //joinFilterMap结果：Map(138 -> (0.9853763650529412,edit), 101 -> (0.8301736332660705,interesting), 0 -> (0.8763523448641077,seha), 88 -> (1.0531784891852904,zoom), 115 -> (0.8387449693767394,voltage), 5 -> (0.8248062793235386,health), 120 -> (

    val bcJoinFilterMap = sc.broadcast(joinFilterMap)

    /* 提取每篇文章的topN label */
    val resDF = tokensDF.map( line => {
      val pagename = line.getAs[String]("pagename")
      val wordsArr = line.getAs[mutable.WrappedArray[String]]("words").toArray.distinct
      var list = List[(Double, String)]()
      /* 2017-04-21 ngram */
      val ngramsArr = line.getAs[mutable.WrappedArray[String]]("ngrams").toSet
      for (ngrams <- ngramsArr) {
        val ngramArr = StringUtils.split(ngrams, ' ')
        if (ngramArr.length == 2) {
          var weight1: Double = 0.0
          val in1 = bcWordMap.value.get(ngramArr(0))
          in1 match {
            case Some(i) => val value = bcJoinFilterMap.value.get(i)
              value match {
                case Some(s) => weight1 = s._1
                case None =>
              }
            case None =>
          }
          var weight2: Double = 0.0
          val in2 = bcWordMap.value.get(ngramArr(1))
          in2 match {
            case Some(i) => val value = bcJoinFilterMap.value.get(i)
              value match {
                case Some(s) => weight2 = s._1
                case None =>
              }
            case None =>
          }
          val weight = weight1 + weight2
          list = (weight, ngrams) :: list
        }
      }

      val resList = list.sortBy(_._1).reverse.take(100)  //降序排序，得出的结果写到一个新的DF里
      var l = List[String]()
      for (res <- resList) {
        l = res._2 :: l
      }
      (pagename, l)
    }).filter(_._2.length > 0).toDF("tr_pn", "tr_label")

    resDF
  }

  /**  计算词与词之间的关系，有关系则加边
    * @param text  每个app的words列
    * @param bcWordMap  经过标号后的wordsMap
    * @param windowSize  后几个词
    * @return
    */
  def getEdgesForText(text: Array[String], bcWordMap: Broadcast[Map[String, Long]], windowSize: Int = 1) = {
    require(windowSize >= 1)
    val textLength = text.size   //words列的词的个数  如：第一条当中词个数为29   [night, drunken, match, perfect, session, end, start, decide, record, keep, sm, delete, choise, get, day, app, start, night, drunk, wake, sm, delete, choice, drunk, sm, receive, send, delete, app]
    val wordMap = bcWordMap.value   //Map(snow -> 40, sens -> 83, health -> 5, savings -> 9, picker -> 68, application -> 37, please -> 70, interesting -> 101, finger -> 3, network -> 35, produce -> 120, method -> 160, flicker -> 31, website -> 90, random -> 10

    val indexCombs = (0 to textLength).flatMap { i =>
      val subList = i+1 to math.min(i+windowSize, textLength-1)
      subList.map(next => (wordMap(text(i)), wordMap(text(next))))
    }

    indexCombs  //Vector((149,14), (149,157), (14,157), (14,96), (157,96), (157,22), (96,22), (96,23), (22,23), (22,93), (23,93), (23,69), (93,69), (93,108), (69,108), (69,131), (108,131), (108,125), (131,125), (131,67), (125,67), (125,58),
  }

  /* 计算中位数 */
  def median[T](s: Seq[T])(implicit n: Fractional[T]) = {
    import n._
    val (lower, upper) = s.sortWith(_<_).splitAt(s.size / 2)  //按从小到大排序，从排序后的list中间分开，形成两个子序列lower和upper
    if (s.size % 2 == 0) (lower.last + upper.head) / fromInt(2) else upper.head    //若序列长为偶数，则中位数=（lower的最后一个数+upper的第一个数）/2，否则为upper的第一个数
  }

  /* 利用ASCII码值来过滤掉词中不是26个字母(大小写)和空格其他字符 */
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
