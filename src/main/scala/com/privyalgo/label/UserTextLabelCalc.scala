package com.privyalgo.label

import com.privyalgo.util.AlgoConstants
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel}
//import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql.{Row, DataFrame}

import scala.collection.mutable

/**
  * 利用用户安装的app列表来计算用户的兴趣标签
  * Created by wuxiushan on 17-4-27.
  */
class UserTextLabelCalc(alDF: DataFrame, userAppDF: DataFrame) extends Serializable{

  val sqlc = userAppDF.sqlContext
  import sqlc.implicits._
  sqlc.setConf("spark.sql.shuffle.partition", "2048")
  def run(): DataFrame = {
    val sc = sqlc.sparkContext
    val appLabelMap = alDF.rdd.map{case Row(pn: String, label: Seq[String]) => (pn, label.mkString(AlgoConstants.ASCII1_SEPERATOR.toString))}.collectAsMap() //.asInstanceOf[Map[String, Seq[String]]]
    val appLabelMapBrc = sc.broadcast(appLabelMap)
    //userAppDF.registerTempTable("user_app")
    //val userApps = sqlc.sql("select client_id, explode(apps) as app from user_app")
    //userApps.write.mode("overwrite").parquet("/user/pangguosheng/test_userprofile/userApps")
    //val userApps = sqlc.read.parquet("/user/pangguosheng/test_userprofile/userApps")
    val DF = userAppDF.map{ case Row(clientId: String, apps: Seq[String]) =>
      val labels = apps.map(app => appLabelMapBrc.value.getOrElse(app, "")).mkString(AlgoConstants.ASCII1_SEPERATOR.toString)
      (clientId, labels)
    }.toDF("client_id", "label")
    //alDF.registerTempTable("app_label")
    /* 将用户安装的app列表与app标签库进行合并 */
    //val joinDF = sqlc.sql(s"select t2.client_id,t1.label from (select * from app_label)t1 join (select client_id, explode(apps) as app from user_app)t2 on t1.pn = t2.app")
    /* 合并用户安装的app列表的所有标签 */
    //val resDF = joinDF.withColumn("label", explode(col("label"))).groupBy(col("client_id")).agg(collect_list(col("label")) as "col_label")
    //resDF.write.mode("overwrite").parquet("/user/pangguosheng/test_userprofile/resDF")
    //System.exit(0)
    //val DF = sqlc.read.parquet("/user/pangguosheng/test_userprofile/resDF")
    val resDF = DF.map{ case Row(clientId: String, labelStr: String) => (clientId, labelStr.split(AlgoConstants.ASCII1_SEPERATOR))}.toDF("client_id", "col_label")

    /* 利用归一化后的tf-idf来计算用户的标签 */
    /* TODO numFeatures默认为2的20次方 */
    val hashingTf = new HashingTF().setInputCol("col_label").setOutputCol("rawFeatures").setNumFeatures(5000000) // 5000000
    val featurizedData = hashingTf.transform(resDF)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel: IDFModel = idf.fit(featurizedData)
    val idfVector = idfModel.idf
    val bcIdfVector = sc.broadcast(idfVector)

    val rescaledData = featurizedData.map(r => {
      val cid = r.getAs[String]("client_id")
      val words = r.getAs[mutable.WrappedArray[String]]("col_label")
      val rawFeatures = r.getAs[SparseVector]("rawFeatures")
      val resVector = transform(bcIdfVector.value, rawFeatures)
      val newResVec = resVector match {
        case Some(v) => v
        case None => null
      }
      (cid, words, newResVec)
    }).filter(l => l._3 != null).toDF("client_id", "col_label", "features")

    /* 对tf*idf值进行排序，取topN的词 */
    val tf = new org.apache.spark.mllib.feature.HashingTF(5000000) //todo 500w是否太大
    val userLabelDF = rescaledData.map {
      line =>
        var wordsIndexMap: Map[String, Int] = Map()
        var valueMap: Map[String, Double] = Map()
        val clientId = line.getAs[String]("client_id")
        val words = line.getAs[mutable.WrappedArray[String]]("col_label")
        for (word <- words) {
          wordsIndexMap += (word -> tf.indexOf(word))
        }
        val features = line.getAs[SparseVector]("features")

        for ((k,v) <- wordsIndexMap) {
          valueMap += (k -> features.apply(v))
        }
        /* 根据value进行降序排序 */
        val resMap = valueMap.toList.sortBy(_._2).reverse
        val resList = resMap.take(200) // todo parameter topN 100

        var labelList:List[String]= List()
        resList.foreach{case(k,v) => labelList = k :: labelList}

        (clientId, labelList)
    }.toDF("client_id", "tag")

    userLabelDF
  }

  /**
    * Transforms a term frequency (TF) vector to a TF-IDF vector with a IDF vector
    * tf和idf分别进行归一化
    *
    * @param idf an IDF vector
    * @param v a term frequence vector
    * @return a TF-IDF vector
    */
  def transform(idf: Vector, v: Vector): Option[Vector] = {
    val n = v.size
    v match {
      case SparseVector(size, indices, values) =>
        val nnz = indices.size
        if (nnz == 0) {
          None
        } else {
          val newValues = new Array[Double](nnz)
          var k = 0
          val normValues = minmaxnorm(values)
          val idfValues = new Array[Double](nnz)
          while(k < nnz) {
            idfValues(k) = idf(indices(k))
            k += 1
          }
          k = 0
          val normIdf = minmaxnorm(idfValues)
          while (k < nnz) {
            newValues(k) = normValues(k) * normIdf(k)
            k += 1
          }
          Some(Vectors.sparse(n, indices, newValues))
        }

      case DenseVector(values) =>
        val newValues = new Array[Double](n)
        var j = 0
        val normValues = minmaxnorm(values)
        val idfValues = new Array[Double](n)
        while(j < n) {
          idfValues(j) = idf(j)
          j += 1
        }
        j = 0
        val normIdf = minmaxnorm(idfValues)
        while (j < n) {
          newValues(j) = normValues(j) * normIdf(j)
          j += 1
        }
        Some(Vectors.dense(newValues))
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }

  /* 归一化 */
  def minmaxnorm(a: Array[Double]) = {
    val min = a.min
    val max = a.max
    if (max == min) {
      a
    } else {
      a.map { x =>
        val v = if (x == min) x + 0.00000001 else x
        (v-min)/(max-min)
      }
    }
  }
}
