package com.basho.spark.connector.perf

import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark._
import com.basho.spark.connector.perf.config.{AmplabConfig, RiakConfig, SparkConfig}
import org.apache.spark.SparkContext



/**
 * @author anekhaev
 */
object SecondaryIndexesPerformanceApp extends App with RiakConfig with SparkConfig with AmplabConfig {

  val riakBucket = config.getString("perf-test.riak.bucket")
  val riakNameSpace = new Namespace("default", riakBucket)

  val sc = new SparkContext(sparkConfig)
  
  val records = sc.riakBucket[String](riakNameSpace)
    .query2iRange("creationNo", 0L, 100L)

  println(s"Received ${records.count()} records")
  
}