package com.basho.riak.spark.util.python

import com.basho.riak.spark._
import com.basho.riak.spark.rdd.RiakRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaRDD
import com.basho.riak.spark.writer.WriteConf
import org.apache.spark.rdd.RDD
import java.util.ArrayList
import scala.collection.JavaConversions._
/**
 * This is a helper class to enable spark connector APIs for python
 *
 * Workaround for  https://issues.apache.org/jira/browse/SPARK-5185
 * requires public zero-arg constructor
 */
class RiakPythonHelper {
  implicit val pickling = new PicklingUtils()
  def riakBucket(jsc: JavaSparkContext, bucketName: String, bucketType: String): RiakRDD[(String, Any)] = {
    jsc.sc.riakBucket(bucketName, bucketType)
  }

  def saveToRiak(jrdd: JavaRDD[Array[Byte]], bucketName: String, bucketType: String) = {
    jrdd.rdd.unpickle().saveToRiak(bucketName, bucketType, WriteConf())
  }

  def query2iKeys[K](jsc: JavaSparkContext, bucketName: String, bucketType: String, index: String, keys: ArrayList[K]) =
    jsc.sc.riakBucket(bucketName, bucketType).query2iKeys(index, keys: _*)

  def queryBucketKeys(jsc: JavaSparkContext, bucketName: String, bucketType: String, keys: ArrayList[String]) =
    jsc.sc.riakBucket(bucketName, bucketType).queryBucketKeys(keys: _*)

  def partitionBy2iRanges[K](jsc: JavaSparkContext, bucketName: String, bucketType: String, index: String, ranges: ArrayList[ArrayList[K]]) = {
    val r = ranges.map(x => (x(0),  x(1))) 
    jsc.sc.riakBucket(bucketName, bucketType).partitionBy2iRanges(index, r: _*)
  }

  def partitionBy2iKeys[K](jsc: JavaSparkContext, bucketName: String, bucketType: String, index: String, keys: K*) = 
    jsc.sc.riakBucket(bucketName, bucketType).partitionBy2iKeys(index, keys: _*)

  def pickleRows(rdd: RDD[_]): RDD[Array[Byte]] = rdd.pickle()

  def javaRDD(rdd: RDD[_]) = JavaRDD.fromRDD(rdd)
}