package com.basho.riak.spark.examples.streaming

import java.util.UUID

import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark.util.RiakObjectConversionUtil
import kafka.serializer.StringDecoder
import com.basho.riak.spark._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Simple demo for Spark streaming job integration.
  * For correct execution:
  *   kafka broker must be installed and running;
  *   'streaming' topic must be created in kafka;
  *   riak kv, kafka and spark master hostnames should be provided as spark configs (or local versions will be used).
  **/
object StreamingKVExample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf(true)
      .setAppName("Simple Spark Streaming to Riak KV Demo")

    setSparkOpt(sparkConf, "spark.master", "local")
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087")
    setSparkOpt(sparkConf, "kafka.broker", "127.0.0.1:9092")

    val sc = new SparkContext(sparkConf)
    val streamCtx = new StreamingContext(sc, Durations.seconds(15))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val namespace = new Namespace("test-data")

    val kafkaProps = Map[String, String](
      "metadata.broker.list" -> sparkConf.get("kafka.broker"),
      "client.id" -> UUID.randomUUID().toString
    )

    KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](streamCtx, kafkaProps, Set[String]("streaming"))
      .foreachRDD { rdd =>
        val rows = sqlContext.read.json(rdd.values).map {
          line => val obj = RiakObjectConversionUtil.to(line)
            obj.setContentType("application/json")
            obj
        }.saveToRiak(namespace)
      }

    streamCtx.start()
    streamCtx.awaitTermination()
    println("Spark streaming context started. Spark UI could be found at http://SPARK_MASTER_HOST:4040")
    println("NOTE: if you're running job on the 'local' master open http://localhost:4040")
  }

  private def setSparkOpt(sparkConf: SparkConf, option: String, defaultOptVal: String): SparkConf = {
    val optval = sparkConf.getOption(option).getOrElse(defaultOptVal)
    sparkConf.set(option, optval)
  }
}
