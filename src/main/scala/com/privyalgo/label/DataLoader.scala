package com.privyalgo.label

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{StringType, StructType}

import scala.reflect.ClassTag


/**
  * Created by wangchanglu on 2017/1/5.
  */
class DataLoader(sc: SparkContext=null, sqlContext: SQLContext=null, spark:SparkSession=null) {

  /* dataFrame Reader based on file format  */
  private def dataFrameReader(format: String): DataFrameReader = {
    if (format == "csv") {
      sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
    } else {
      sqlContext.read.format(format)
    }
  }

  /* `format` must be orc| json | csv */
  private def checkFileFormat(format: String): Unit = {
    require(format == "parquet" || format == "orc" || format == "json" || format == "csv",
      s"fileFormat found: $format, format must be one of {'csv', 'json', 'orc', 'parquet'}")
  }

  /* dataFrame Writer based on file format */
  private def dataFrameWriter(df: DataFrame, format: String, header: Boolean = true): DataFrameWriter[Row] = {
    if (format == "csv") {
      if (header)
        df.write.format("com.databricks.spark.csv").option("header", "true")
      else
        //if write to redshift, we need csv without header
        df.write.format("com.databricks.spark.csv").option("header", "false")
    } else {
      df.write.format(format)
    }
  }

  /*
   * read base records and select attributes 'domain
   * TODO: Throw error when read fails
   */
  def read(format: String, inputPath: String, schema: StructType = null): DataFrame = {
    checkFileFormat(format)
    val reader = if (schema != null) {
      dataFrameReader(format).schema(schema)
    } else {
      dataFrameReader(format)
    }
    reader.load(inputPath)
  }


  /*
   * write dataframe to file
 	 * Throws error when write fails
   */
  def write(df: DataFrame, outFormat: String, outputPath: String,
            overwrite: Boolean = false, header: Boolean = true): Unit = {
    checkFileFormat(outFormat)
    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
    dataFrameWriter(df, outFormat, header).mode(saveMode).save(outputPath)
  }

  /*
   * write dataframe to file
   * appends dataframe to file if the file already exists
   */
  def append(df: DataFrame, outFormat: String, outputPath: String,
             overwrite: Boolean = false, header: Boolean = true): Unit = {
    checkFileFormat(outFormat)
    val saveMode = SaveMode.Append
    dataFrameWriter(df, outFormat, header).mode(saveMode).save(outputPath)
  }

  /**
    * load streaming data from socket as DataFrame.
    * Check the 'Unsupported Operations' part of structured-streaming-doc for
    * more information about which operations can(or can't) be applied.
    * structured-streaming-doc:
    *   https://spark.apache.org/docs/2.2.0/structured-streaming-programming-guide.html
    *
    * @param host socket host, (e.g. 192.168.0.253)
    * @param port socket port, (e.g. 8989)
    * @return DataFrame with one column "value" | didn't support action-operations (e.g. .count .show .rdd)
    */
  def streamingReadSocket(host:String,port:String):DataFrame={
    val optMap = Map("host"->host,"port"->port)
    spark.readStream.format("socket").options(optMap).load()
  }

  /**
    * load streaming data from kafka topics as DataFrame.
    * Currently, this func only support subscription of SINGLE topic.
    * check the kafka-integration-guide for more information.
    * kafka-integration-guide :
    *   https://spark.apache.org/docs/2.1.2/structured-streaming-kafka-integration.html
    * p.s.
    *   add this conf when spark-submit(or spark-shell):
    *     --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.2
    *   add this export:
    *     export SPARK_KAFKA_VERSION=0.10
    *
    * @param topic topic to subscribe, (e.g. test.rec)
    * @param producer kafkaProducer's host:port, (e.g. test-datanode002.yf.apus.com:9092)
    * @param startingOffsets "latest" or "earliest"
    * @return DataFrame most useful columns are [key:String | value:String | timestamp:timestamp]
    */
  def streamingReadKafka(topic:String, producer:String, startingOffsets:String="latest",inp_options:Map[String,String]=Map.empty[String,String]):DataFrame = {
    val ori_optionMap = Map("kafka.bootstrap.servers"->producer,
      "subscribe"->topic,
      "startingOffsets"->startingOffsets,
      "minPartitions"->"10",
      "failOnDataLoss"->"false"
    )
    val optionMap = ori_optionMap ++ inp_options
    spark.readStream.format("kafka")
      .options(optionMap)
      .load()
      .withColumn("key",col("key").cast(StringType))
      .withColumn("value",col("value").cast(StringType))
  }

  /**
    * memory output, save the processed streaming-data in table
    *
    * @param streamingDF processed df
    * @param tableName processed df will be saved at this table
    * @param processingTime the interval of triggers, 0 means ASAP. unit is LongMs
    */
  def streamingWriteToMemory[T:ClassTag](streamingDF:Dataset[T], tableName:String, processingTime:Long=0*1000):StreamingQuery={
    val query = {
      streamingDF.writeStream
        .outputMode(OutputMode.Append)
        .queryName(tableName)
        .format("memory")
        .trigger(Trigger.ProcessingTime(processingTime))
        .start()
    }
    query
  }

  def streamingWriteToConsole[T:ClassTag](streamingDF:Dataset[T],truncate:Boolean=true):StreamingQuery={
    val optionsMap = Map("truncate"->truncate.toString)
    val query = {
      streamingDF.writeStream
        .format("console")
        .options(optionsMap)
        .outputMode(OutputMode.Update)
        .start()
    }
    query
  }

  /**
    * streaming sink for kafka. (put the data to kafka topics)
    * (brokers can be assigned to any host available)
    *
    * @param streamingDF processed dataset;
    *                    IMPORTANT: must be two StringType column, as [key | value]
    * @param topic  topic of kafka-producer (e.g. test.keywords)
    * @param brokers  brokers of kafka-producer (e.g. test-datanode001.yf.apus.com:9092)
    */
//  def streamingWriteToKafka(streamingDF:Dataset[String], topic:String, brokers:String,checkpointPath:String=null,processingTime:Long=0*1000L,queryName:String="kafkaWriter"):StreamingQuery = {
//    // 以下两个implicit是为了解决 .as[(String,String)]报错的问题(Unable to find encoder for type stored in a Dataset)
//    implicit def single[A](implicit c: ClassTag[A]): Encoder[A] = Encoders.kryo[A](c)
//    implicit def tuple2[A1, A2](implicit e1: Encoder[A1],e2: Encoder[A2]): Encoder[(A1, A2)] = Encoders.tuple[A1, A2](e1, e2)
//
//    val wrr = new ForeachWriter[String] {
//      // 相关配置
//      val kafkaProperties = new Properties()
//      kafkaProperties.put("bootstrap.servers", brokers)
//      kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//      kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//      // 初始化
//      var producer_ : KafkaProducer[String,String] = _
//      override def open(partitionId: Long, version: Long): Boolean = {
//        producer_ = new KafkaProducer[String,String](kafkaProperties)
//        true
//      }
//
//      override def process(value: String): Unit = {
//        producer_.send(new ProducerRecord(topic, value))
//      }
//
//      override def close(errorOrNull: Throwable): Unit = {
//        producer_.close()
//      }
//    }

//    val kafka_foreachQuery ={
//      if(checkpointPath==null){
//        println("[***WARN***] 不推荐kafka写入时不保存ckpt,最好传入checkpointPath参数")
//        streamingDF.writeStream.queryName(queryName)
//          .outputMode(OutputMode.Append)
//          .trigger(Trigger.ProcessingTime(processingTime))
//          .foreach(wrr).start()
//      }else{
//        streamingDF.writeStream.queryName(queryName)
//          .option("checkpointLocation",checkpointPath)
//          .trigger(Trigger.ProcessingTime(processingTime))
//          .outputMode(OutputMode.Append)
//          .foreach(wrr).start()
//      }
//    }
//    kafka_foreachQuery
//  }

  /**
    * streaming sink for parquet format.
 *
    * @param streamingDF processed dataset;
    * @param savePath parquet file save path;
    * @param checkpointPath path to save streaming-checkpoint
    */
  def streamingWriteToParquet[T:ClassTag](streamingDF:Dataset[T],
                                          savePath:String,
                                          checkpointPath:String,
                                          partitionBy:Seq[String]=Seq.empty[String],
                                          processingTime:String="5 minutes",
                                          queryName:String="parquetWriter"):StreamingQuery = {
    val tmp = {
      streamingDF.writeStream.queryName(queryName)
        .format("parquet")
        .option("path", savePath)
        .option("checkpointLocation",checkpointPath)
        .outputMode(OutputMode.Append)
        .trigger(Trigger.ProcessingTime(processingTime))
    }
    if(partitionBy.isEmpty) tmp.start() else tmp.partitionBy(partitionBy:_*).start()
  }

  def streamingWriteToKafka_new(streamingDF:Dataset[(String,String)], topic:String, brokers:String,checkpointPath:String=null,queryName:String="kafkaWriter"):StreamingQuery = {
    require(streamingDF.sparkSession.version >= "2.2.0","低于2.2.0的版本API不支持")
    val optMap = scala.collection.mutable.Map("kafka.bootstrap.servers"->brokers,"topic"->topic)
    if(checkpointPath!=null){
      optMap.update("checkpointLocation",checkpointPath)
    }else println("[***WARN***] 不推荐kafka写入时不保存ckpt,最好传入checkpointPath参数")
    streamingDF.writeStream.queryName(queryName)
      .format("kafka")
      .queryName("kafkaStream")
      .options(optMap)
      .start()
  }

  // todo | 自定义(参数重载 or 变长参数 or Map参数 or 新的函数名)方式加载或输出数据
//  def streamingOutput(streamingDF:DataFrame,savePath:String) ={
//    val query = {
//      streamingDF.writeStream
//        .outputMode("append")
//        .format("parquet")
//        .option("path", savePath)
//        .option("checkpointLocation", "/mnt/sample/check")
//        .start()
//    }
//    query.awaitTermination()
//  }

  def readMongo(url:String):DataFrame = {
    val optMap = Map(
      "spark.mongodb.input.uri" -> url,
      "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
      "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
      "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32",
      "mergeSchema"->"true"
    )
    spark.read.format("com.mongodb.spark.sql").options(optMap).load()
  }



}