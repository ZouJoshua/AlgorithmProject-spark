package com.privyalgo.nlp

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.functions.{col, length, split}


/**
 * Created by Joshua on 2020/12/17
 */
object Word2vecProcess {

    def main(args: Array[String]): Unit = {
        val appName = "Train-Word2vec"
        val spark = SparkSession.builder()
          .master("local")
          .appName(appName)
          .getOrCreate()
        val sc = spark.sparkContext

        val path = "/region04/27367/app/develop/nlp/yxy/gnn/push/train_data/push_data_uiu_iitii_uitiu.txt"
//        val sample_path = "/region04/27367/app/develop/nlp/yxy/gnn/push/train_data/sample.txt"
        val all_item_df = spark.read.text(path)
        val doc_df = all_item_df.withColumn("items", split(col("value"), " ")).filter("size(items) > 1")
        doc_df.cache()
        val word2Vec = new Word2Vec().setInputCol("items").setOutputCol("result").setVectorSize(10).setMinCount(1).setNumPartitions(200).setMaxSentenceLength(100)

        val model = word2Vec.fit(doc_df)
        model.getVectors.show(false)

        val result = model.transform(doc_df)
        result.select("result").take(3).foreach(println)




    }
}
