package com.privyalgo.nlp

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.reflect.ClassTag

/**
  * Created by Joshua on 2019-02-01
  */
object LightldaPreProcess {

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

  // Seq 类型的分片广播
  def multiBroadcast[T:ClassTag](sc:SparkContext, value:Seq[T]) : Seq[Broadcast[Seq[T]]]= {
    val eachSize:Long=100*10000
    val e_size = (estimateSize(value)/1024.0).formatted("%.3f").toDouble //GB
    if(e_size > 5.0) println(s">>> estimate size ($e_size GB) exceed 5GB, it's recommended to use 'join'.")

    var broadcast_seq = Seq.empty[Broadcast[Seq[T]]]
    val value_toSplit = value.zipWithIndex
    for(i <- 0L to value.size/eachSize){
      println("broadcasting %s".format(i))
      val bd = sc.broadcast(value_toSplit.filter(x => x._2/eachSize == i).map(_._1))
      broadcast_seq = broadcast_seq :+ bd
    }
    broadcast_seq
  }

  // 预估变量的大小
  def estimateSize(value:AnyRef, verbose:Boolean=false) = {
    import org.apache.spark.util.SizeEstimator
    // 多少mb
    val size = SizeEstimator.estimate(value)/1024.0/1024
    if(verbose) println(s"estimate size is $size MB")
    size
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Lightlda_data_preprocess")
      .getOrCreate()
    val sc = new SparkContext()
    import spark.implicits._

    /**
      * lightlda数据预处理
      * lightlda训练模型需要两个文件(UCI格式的文件)
      * 1. docword.news_content.txt （"docID"|"wordID"|"wordTF"） ===> lightlda_docword
      * 2. vocab.news_content.txt （按照词id排列的词汇总量集合（唯一）） ===> lightlda_vocab
      * 或者直接用以上两个文件生成的libsvm文件和dict文件
      * 1.news_content.libsvm  (docID wordID1:wordTF wordID2:wordTF ...)文档号从1开始代指某篇文档，词id以0开始，与dict对应，次数为文档号对应的文档中该词出现的次数
      * 2.news_content.word_id.dict （wordID word wordTF）出现总数是所有文档出现总数
      */

    //------------------------------------ 1 路径设置 -----------------------------------------
    val dt = "2019-02-01"

    // 分词结果
    val word_ngrams = "/user/zhoutong/id_NERs_textFile"


    //------------------------------------ 2 生成文章词库vocab -----------------------------------------

    val vocab_DF = spark.read.textFile(word_ngrams)
    val vocab = vocab_DF.filter(!_.contains("<")).filter(!_.contains(";")).filter(!_.contains("=")).filter(!_.contains(",")).map{
      x =>
        val id_and_ngrams = x.split("\t")
        val split_num = id_and_ngrams.size
        val id = id_and_ngrams(0).trim().toString
        var ngrams = Array[String]()
        if(split_num == 2) {
            ngrams = id_and_ngrams(1).replaceAll("-LRB-", "").replace("-RRB-", "").replaceAll("\\s+", " ").split(" ")
            ngrams
        }
        (id, ngrams)
    }.toDF("docID","ngrams").filter("size(ngrams) > 0").dropDuplicates("docID")  // 过滤掉分词为0、重复id的文章

//    val vocab_tf_less_10 = {
//      val vocab_filter = vocab.select("ngrams").rdd.flatMap(r => r.getAs[Seq[String]]("ngrams"))
//        .map(word=>(word,1)).reduceByKey(_ + _).toDF().filter("_2 < 10")
//      vocab_filter.map(_.getAs[String]("_1")).collect().toSet
//    }

    val vocab_tf_less_10 = {
      vocab.select("ngrams").rdd.flatMap(r => r.getAs[Seq[String]]("ngrams"))
        .map(word=>(word,1)).reduceByKey(_ + _)
        .toDF("word","count").filter("count < 10")
    }
    val vocab_id = {
      vocab.selectExpr("docID","explode(ngrams) as word").map{
        row =>
          val id = row.getAs[String]("docID")
          val word = row.getAs[String]("word").replaceAll("\\-.+\\-","")
          (id,word)
      }.toDF("docID","word")
    }

    val vocab_tmp = vocab_id.join(vocab_tf_less_10, Seq("word"),"left").filter("word != ''").filter("count is null")

    val vocab_filtered1 = vocab_tmp.groupBy("docID").agg(collect_list(expr("word")).as("ngrams_filtered"))
    // 先过滤掉词频低于10，写入文件
    vocab_filtered1.write.mode("overwrite").save("news_lightlda/docid_ngrams")

//    val vocab_filtered = vocab_tmp.rdd.map{
//      row =>
//        (row.getAs[String]("docID"), row.getAs[String]("word"))
//    }.groupByKey().mapValues(List(_))

//    val vocab_tf_less_10_bd= sc.broadcast(vocab_tf_less_10)
//    val vocab_filtered = {
//      vocab.map{
//        row =>
//          val did = row.getAs[String]("docID")
//          val ngrams = row.getAs[Seq[String]]("ngrams")
//          var ngrams_new = Seq.empty[String]
//          for(word <- ngrams){
//            if(!vocab_tf_less_10_bd.value.contains(word)){
//              ngrams_new = ngrams_new :+ word
//            }
//          }
//          (did,ngrams_new)
//      }.toDF("docID", "ngrams_filtered")
//    }


    val df = spark.read.parquet("news_lightlda/docid_ngrams")

    val vocab_new = {
      val vocab_sort = df.select("ngrams_filtered").rdd.flatMap(r => r.getAs[Seq[String]]("ngrams_filtered"))
        .map(word=>(word,1)).reduceByKey(_ + _).toDF().sort("_1")
      val id_vocab = dfZipWithIndex(vocab_sort).map{
        r =>
          val id = r.getAs[Long]("id") - 1  // 词id 以0开始
          val word = r.getAs[String]("_1")
          val word2id = Map( word -> id)
          (id,word,word2id)
      }.toDF("wordID","word","word2id")
      id_vocab
      }
    vocab_new.write.mode("overwrite").save("news_lightlda/docid_word_map")

    val vocab_new_df = spark.read.parquet("news_lightlda/docid_word_map")
    val vocab_save = vocab_new_df.coalesce(1).sort("wordID").select("word")
    // 保存文章词库
    vocab_save.write.mode("overwrite").text("news_lightlda/lightlda_vocab")

    //------------------------------------ 2 计算每篇文章词的词频 -----------------------------------------

    val df_docid = dfZipWithIndex(df)
    val word_flat_UCI = df_docid.selectExpr("id as docID","docID as docID_ori", "explode(ngrams_filtered) as word")

    val word_tfidf_UCI = word_flat_UCI.join(vocab_new_df, Seq("word"))
    val word_filtered = word_tfidf_UCI.groupBy("docID", "wordID").agg(count("wordID").as("tf"))
    word_filtered.write.mode(SaveMode.Overwrite).parquet("news_lightlda/tmp/lightlda_docword")

    val word_all = spark.read.parquet("news_lightlda/tmp/lightlda_docword")

    val lightlda_docword_filtered = word_all.coalesce(10).sort("docID").map{
      r =>
        val did = r.getAs[Long]("docID")
        val wid = r.getAs[Long]("wordID")
        val tf = r.getAs[Long]("tf")
        val txt = did + "|" + wid + "|" + tf
        txt.toString
    }
    // 保存文件lightlda_docword
    lightlda_docword_filtered.coalesce(10).write.mode(SaveMode.Overwrite).text("news_lightlda/lightlda_docword")


    //------------------------------------ 3 直接生成libsvm文件和dict文件 -----------------------------------------

    val word_dict = {
      val word_sorted = word_tfidf_UCI.groupBy("wordID", "word").agg(count("word").as("tf")).coalesce(10).sort("wordID")
      val word_sorted_str = word_sorted.map{
        r =>
          val wid = r.getAs[Long]("wordID")
          val word = r.getAs[String]("word")
          val tf = r.getAs[Long]("tf")
          val txt = wid + "\t" + word + "\t" + tf
          txt.toString
      }
      word_sorted_str
    }
    word_dict.coalesce(1).write.mode(SaveMode.Overwrite).text("news_lightlda/lightlda_dict")


    val word_libsvm = {
      val libsvm_sorted = word_tfidf_UCI.groupBy("docID", "wordID").agg(count("wordID").as("tf")).coalesce(10).sort("docID")
      val libsvm = libsvm_sorted.map{
        r =>
          val did = r.getAs[Long]("docID")
          val wid_tf = r.getAs[Long]("wordID").toString + ":" + r.getAs[Long]("tf").toString
          (did, wid_tf)
      }.toDF("docID", "wordID_tf")
      val doc_libsvm = libsvm.groupBy("docID").agg(collect_list(expr("wordID_tf")).as("ngrams_id_tf")).coalesce(1).sort("docID")
      val out = doc_libsvm.map{
        r =>
          val did = r.getAs[Long]("docID")
          val ngrams = r.getAs[Seq[String]]("ngrams_id_tf")
          val result = did.toString + "\t" + ngrams.mkString(" ")
          result
      }
      out
    }
    word_libsvm.coalesce(1).write.mode(SaveMode.Overwrite).text("news_lightlda/lightlda_libsvm")


  }
}
