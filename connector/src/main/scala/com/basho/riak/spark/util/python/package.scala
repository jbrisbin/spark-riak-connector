package com.basho.riak.spark.util

import com.basho.riak.spark.util.python.PicklableRDD
import com.basho.riak.spark.util.python.UnpicklableRDD
import org.apache.spark.rdd.RDD

package object python {

  implicit def toPickleableRDD(rdd: RDD[_]) = new PicklableRDD(rdd)
  implicit def toUnpickleableRDD(rdd: RDD[Array[Byte]]) = new UnpicklableRDD(rdd)

}
