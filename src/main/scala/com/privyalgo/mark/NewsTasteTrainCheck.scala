package com.privyalgo.mark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
  * Created by Joshua on 2019-06-17
  */
object NewsTasteTrainCheck {
  def main(args: Array[String]): Unit = {

    //------------------------------------1 浏览口味数据清除重新训练模型 -----------------------------------------
    //
    def taste_update_check(spark: SparkSession) = {
      val df = spark.read.json("taste_check/*_check_pred.json")
      val drop1 = df.filter("taste = predict_taste").filter("predict_taste_proba < 0.7").select("id")
      val drop2 = df.filter("taste != predict_taste").filter("predict_taste_proba > 0.5").select("id")
      val drop_df = drop1.union(drop2).withColumn("drop", lit(1))
      val new_ori = df.join(drop_df, Seq("id"), "left").filter("drop is null").drop("drop","predict_taste","predict_taste_proba")
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("taste_check/train_corpus_taste_v1")
    }


  }
}