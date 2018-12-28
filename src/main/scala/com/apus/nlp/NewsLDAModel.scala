package com.apus.nlp

import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by Joshua on 2018-11-17
  */

object NewsLDAModel {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("LDAModel-nlp")
      .getOrCreate()
    val sc = new SparkContext()
    import spark.implicits._

    val ngramsPath = "news_content/lda_libsvm/dt=2018-11-20"

    // Loads data.
    val dataset = spark.read.option("numFeatures", "15984963").format("libsvm").load(ngramsPath)

    import org.apache.spark.ml.linalg.SparseVector
    dataset.head.getAs[SparseVector]("features").indices.length
    dataset.head.getAs[SparseVector]("features").values.length
    dataset.map{
      r =>
        val label = r.getAs[Double]("label")
        val num1 = r.getAs[SparseVector]("features").indices.length
        val num2 = r.getAs[SparseVector]("features").values.length
        val x = if(num1 < 20) 1 else 0
        (label, x)
    }.toDF("x1","x2")


    //------------------------------------1 模型训练-----------------------------------------
    /**
      * k: 主题数，或者聚类中心数
      * DocConcentration：文章分布的超参数(Dirichlet分布的参数)，必需>1.0，值越大，推断出的分布越平滑
      * TopicConcentration：主题分布的超参数(Dirichlet分布的参数)，必需>1.0，值越大，推断出的分布越平滑
      * MaxIterations：迭代次数，需充分迭代，至少20次以上
      * setSeed：随机种子
      * CheckpointInterval：迭代计算时检查点的间隔
      * Optimizer：优化计算方法，目前支持"em", "online" ，em方法更占内存，迭代次数多内存可能不够会抛出stack异常
      */
    val lda=new LDA().setK(500).setTopicConcentration(3).setDocConcentration(3).setOptimizer("online").setCheckpointInterval(2).setMaxIter(100)

    val model=lda.fit(dataset)

    /**生成的model不仅存储了推断的主题，还包括模型的评价方法。*/
    //---------------------------------2 模型评价-------------------------------------

    //模型的评价指标：ogLikelihood，logPerplexity
    //（1）根据训练集的模型分布计算的log likelihood，越大越好。
    val ll = model.logLikelihood(dataset)

    //（2）Perplexity评估，越小越好
    val lp = model.logPerplexity(dataset)

    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound bound on perplexity: $lp")

    //---------------------------------3 模型及描述------------------------------
    //模型通过describeTopics、topicsMatrix来描述

    //（1）描述各个主题最终的前maxTermsPerTopic个词语（最重要的词向量）及其权重
    val topics=model.describeTopics(maxTermsPerTopic=10)
    println("The topics described by their top-weighted terms:")
    topics.show(false)


    /**主题    主题包含最重要的词语序号                     各词语的权重
        +-----+-------------+------------------------------------------+
        |topic|termIndices  |termWeights                               |
        +-----+-------------+------------------------------------------+
        |0    |[5, 4, 0, 1] |[0.21169509638828377, 0.19142090510443274]|
        |1    |[5, 6, 1, 2] |[0.12521929515791688, 0.10175547561034966]|
        |2    |[3, 10, 6, 9]|[0.19885345685860667, 0.18794498802657686]|
        +-----+-------------+------------------------------------------+
      */

    //（2） topicsMatrix: 主题-词分布，相当于phi。
    val topicsMat=model.topicsMatrix
    model.estimatedDocConcentration
    model.getTopicConcentration
    println("topicsMatrix")
    println(topicsMat.toString())
    /**topicsMatrix
        12.992380082908886  0.5654447550856024  16.438154549631257
        10.552480038361052  0.6367807085306598  19.81281695100224
        2.204054885551135   0.597153999004713   6.979803589429554
      *
      */

    //-----------------------------------4 对语料的主题进行聚类---------------------
    val topicsProb=model.transform(dataset)
    topicsProb.select("label", "topicDistribution").show(false)

    /** label是文档序号 文档中各主题的权重
        +-----+--------------------------------------------------------------+
        |label|topicDistribution                                             |
        +-----+--------------------------------------------------------------+
        |0.0  |[0.523730754859981,0.006564444943344147,0.46970480019667477]  |
        |1.0  |[0.7825074858166653,0.011001204994496623,0.206491309188838]   |
        |2.0  |[0.2085069748527087,0.005698459472719417,0.785794565674572]   |
        ...

      */

    //-----------------------------------5 模型保存与加载--------------------------
    model.save("")
    spark.stop()
  }
}
