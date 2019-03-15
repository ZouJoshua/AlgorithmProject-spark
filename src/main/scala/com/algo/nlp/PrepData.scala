package com.algo.nlp


import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
//import org.apache.spark.ml.linalg.Vectors
//import org.apache.spark.ml.feature.LabeledPoint

/**
  * Created by Joshua on 2018-11-24
  */
object PrepData {

  // 对libsvm文件word索引排序

  def trans(line: String): String = {
    //以第一条为例
    //ne: String = 0 2:1 1:4 6:4 7:37 8:37 9:0 10:10 11:0 12:0 ...
    //以空格分割
    val cols = line.split(" ")
    //把标签和特征分开
    val label = cols(0)
    val features = cols.slice(1, cols.length)
    //对特征进行map，将下标和值分开，并用下标进行排序
    //newFeatures: Array[(Double, Double)] = Array((1.0,4.0), (2.0,1.0), (6.0,4.0), (7.0,37.0), (8.0,37.0)....
    val newFeatures = features.map(l => {
      val k = l.split("\\:")
      //转为double，用于排序
      (k(0).toDouble, k(1).toDouble)
    }).sortBy(_._1)
    //重组字符串，，加上标签，并用空格分隔
    //result: String = 0 1:4.0 2:1.0 6:4.0 7:37.0 8:37.0 9:0.0 10:10.0....
    val result = label + " " + newFeatures.map(s => s._1.toInt + ":" + s._2).mkString(" ")
    result
  }

  // 转化为lda-libsvm
  def trans2labeledpoint(line: String): LabeledPoint = {
    //以第一条为例
    //ne: String = 0 2:1 1:4 6:4 7:37 8:37 9:0 10:10 11:0 12:0 ...
    //以空格分割
    val cols = line.split(" ")
    //把标签和特征分开
    val label = cols(0)
    val features = cols.slice(1, cols.length)
    //对特征进行map，将下标和值分开，并用下标进行排序
    //newFeatures: Array[(Double, Double)] = Array((1.0,4.0), (2.0,1.0), (6.0,4.0), (7.0,37.0), (8.0,37.0)....
    val sortFeatures = features.map(s => {
      val k = s.trim().split("\\:")
      //转为double，用于排序
      (k(0).toInt, k(1).toDouble)
    }).sortBy(_._1)
    //转为LabeledPoint
    //result: org.apache.spark.ml.linalg.Vector = (3,[0,2],[1.0,3.0])
    LabeledPoint(label.toLong, Vectors.sparse(15984963, sortFeatures))
    // LabeledPoint(label, Vectors.dense()) 密度矩阵，零值也存储
    }

  // 增加索引列
  def dfZipWithIndex(df: DataFrame,
                     offset: Int = 1,
                     colName: String = "id",
                     inFront: Boolean = true
                    ) : DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(
          (if (inFront) Seq(ln._2 + offset) else Seq())
            ++ ln._1.toSeq ++
            (if (inFront) Seq() else Seq(ln._2 + offset))
        )
      ),
      StructType(
        (if (inFront) Array(StructField(colName,LongType,false)) else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]() else Array(StructField(colName,LongType,false)))
      )
    )
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("PrepData-nlp")
      .getOrCreate()
    val sc = new SparkContext()
    import spark.implicits._

    /**
      * lightlda数据预处理
      * lda数据预处理
      */

    //------------------------------------ 1 路径设置 -----------------------------------------
    val dt = "2018-11-22"
    // 22-26日分词
    val ngramsPath = "/user/zoushuai/news_content/docword"
    // val ngramsPath = "/user/zoushuai/news_content/docword/dt=%s".format(dt)
    val vocabPath = "/user/zoushuai/news_content/vocab/dt=%s".format(dt)
    // 22-26日所有词汇
    val vocabAllPath = "/user/zoushuai/news_content/vocab/all"
    val word_vocab_AllPath = "news_content/word_vocab/all"
    // 22-24日分词tfidf值
    val tfidfPath = "/user/zoushuai/news_content/tfidf/dt=%s".format(dt)
    val tfidfAllPath = "/user/zoushuai/news_content/tfidf/all"

    //------------------------------------ 2 生成文章词库vocab -----------------------------------------
    //    val vocab_DF = spark.read.parquet(vocabPath)
    //    val vocab = vocab_DF.map{
    //      x =>
    //        val word_id = x.getAs[String]("value").split("\\t")
    //        (word_id(0).trim(), word_id(1).toLong)
    //    }.toDF("word","wordID")

    val vocab_DF = spark.read.textFile(vocabAllPath)
    val vocab = vocab_DF.map{
      x =>
        val word_and_id = x.split("\t")
        (word_and_id(0).trim(), word_and_id(1).toLong)
    }.toDF("word","wordID")
//    val vocab_save = vocab.sort("wordID").select("word")
    val vocab_save = vocab.repartition(1).sort("wordID").select("word").repartition(1)
    vocab_save.cache
    //    val vocab_save = vocab.sort(desc("wordID")).select("word")
    // 保存文章词库
    vocab_save.write.mode(SaveMode.Overwrite).text(word_vocab_AllPath)

    //------------------------------------ 2 读取tfidf -----------------------------------------

    // 22-24日第一次时做法
    // 取tf_idf值大于某一阈值的词
    val docword_tfidf_DF = spark.read.parquet(tfidfPath)
    val word_tfidf_UCI = docword_tfidf_DF.flatMap{
      r =>
        val docID = r.getAs[String]("article_id")
        //        val tfidf_ngrams = r.getAs[Seq[(Long,Double)]]("tf_Mul_idf")
        val tfidf_ngrams:Seq[(Long,Double)] = r.getAs[Seq[Row]]("tf_Mul_idf").map(x => {(x.getLong(0), x.getDouble(1))})
        var out = Seq.empty[(String, Long, Double)]
        for(tfidf <- tfidf_ngrams){
          out = out :+ (docID.toString, tfidf._1, tfidf._2)
        }
        out
    }.toDF("docID","wordID","wordTFIDF")
    val word_filtered = word_tfidf_UCI.drop("wordTFIDF").groupBy("docID", "wordID").agg(count("wordID").as("tf"))
    val lightlda_docword_filtered = word_filtered.map{
      r =>
        val did = r.getAs[String]("docID")
        val wid = r.getAs[Long]("wordID")
        val tf = r.getAs[Long]("tf")
        val txt = did + "|" + wid + "|" + tf
        txt.toString
    }
    // 保存文件lightlda
    lightlda_docword_filtered.repartition(1).write.mode(SaveMode.Overwrite).text("news_content/lightlda_docword_filtered/dt=2018-11-20")

    // 22-26日所有tfidf
    val docword_tfidf_allDF = {
      spark.read.parquet(tfidfAllPath)
        .withColumnRenamed("word","wordID")
        .withColumnRenamed("idf","wordTFIDF")
    }

    //------------------------------------ 3 进行词筛选 -----------------------------------------
    // 进行词筛选（选出每篇文章长度大于300，命中词大于15个的）

    // val docwordDF = spark.read.parquet(ngramsPath)  // 单日计算
    val docwordDF = spark.read.option("basePath", ngramsPath).parquet("/user/zoushuai/news_content/docword/dt=2018-11-2[2-6]")
    val docwordAll = {
      docwordDF
        .selectExpr("article_id", "article_parsed as article", "content_ngram_idx", "size(content_ngram_idx) as gram_size", "length(article_parsed) as article_length")
    }
    val ngram_size = docwordAll.rdd.map{
      r =>
        val id  = r.getAs[String]("article_id")
        val ngram = r.getAs[Seq[Long]]("content_ngram_idx").toSet.size
        (id,ngram)
    }.toDF("article_id", "ngram_size_new")

    val  docwordFiltered  = {
      docwordAll.join(ngram_size, Seq("article_id"))
        .filter("article_length > 300")
        .filter("ngram_size_new > 20 and ngram_size_new < 200")
    }
    // 过滤掉tfidf值较大和较小的(查看vocab的词)
    val vocab_tfidf = vocab.join(docword_tfidf_allDF,Seq("wordID"))

    // 增加索引列，并增加docid映射
    val docwordIndex = dfZipWithIndex(docwordFiltered)
    val idmapUDF = udf{(id:Long, article_id:String) => Map(id -> article_id)}
    val docwordIndexMap = docwordIndex.withColumn("idmap", idmapUDF(col("id"),col("article_id")))
    docwordIndexMap.cache

    //------------------------------------ 4 生成lightlda-docword文件 -----------------------------------------
    // 生成lightlda-docword文件（UCI格式）
    val word_UCI = docwordIndexMap.flatMap{
      r =>
        val docID = r.getAs[Long]("id")
        val ngrams = r.getAs[Seq[Long]]("content_ngram_idx")
        var out = Seq.empty[(Long, Long)]
        for(wordID <- ngrams){
          out = out :+ (docID,wordID)
        }
        out
    }.toDF("docID","wordID")
    val word_save_tmp = word_UCI.groupBy("docID", "wordID").agg(count("wordID").as("tf")).sort("docID","wordID").coalesce(1)
    word_save_tmp.cache
    // 生成lightlda-docword文件
    val lightlda_docword = word_save_tmp.map{
      r =>
        val did = r.getAs[Long]("docID")
        val wid = r.getAs[Long]("wordID")
        val tf = r.getAs[Long]("tf")
        val txt = did + "|" + wid + "|" + tf
        txt.toString
    }
    lightlda_docword.write.mode(SaveMode.Overwrite).text("news_content/lightlda_docword/all_v2")

    /*
    //------------------------------------ 5 生成lda-libsvm格式数据 -----------------------------------------
    // 生成lda-libsvm格式数据
    val word_libsvm_RDD = word_save_tmp.rdd.map{
      r =>
        val did = r.getAs[String]("docID")
        // wordID索引加1，索引从1开始，以升序排列。加载libsvm时，特征的索引会被转换成从0开始
        val wid = r.getAs[Long]("wordID") + 1
        val tf = r.getAs[Long]("tf")
        (did.toString, wid + ":" + tf)
    }
    val word_libsvm = word_libsvm_RDD.reduceByKey(_ + " " + _).map(r => r._1 + " " + r._2).toDF("data")

    val word_libsvm_sorted = word_libsvm.map(line => trans(line.getAs[String]("data")))
    val word_libsvm_vectors = word_libsvm.map(line => trans2labeledpoint(line.getAs[String]("data"))).toDF("label","features")
    val word_labeledpoint_RDD = word_libsvm.rdd.map(line => trans2labeledpoint(line.getAs[String]("data")))

    /* 保存lda-libsvm */
    word_libsvm_sorted.repartition(1).write.mode(SaveMode.Overwrite).text("news_content/lda_libsvm/dt=2018-11-20")

    /* 向量格式写数据 */
    // spark2.2.0 ml库有bug  java.util.NoSuchElementException: key not found: numFeatures
    // spark2.2.1 和2.3.0修复此bug
    word_libsvm_vectors.repartition(1).write.format("libsvm").mode(SaveMode.Overwrite).save("news_content/lda_libsvm/dt=2018-11-20")

    /* 保存libsvm格式文件 */
    // spark2.2.0 mllib库 可以保存成功
    MLUtils.saveAsLibSVMFile(word_labeledpoint_RDD,"news_content/lda_libsvm/dt=2018-11-20")
    */
  }
}
