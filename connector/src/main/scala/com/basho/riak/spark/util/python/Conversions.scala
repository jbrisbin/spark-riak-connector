package com.basho.riak.spark.util.python

import java.util.{List => JList}
import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer
import java.util.{List => JList}

object Conversions {
  def asArray[T: ClassTag](c: Any): Array[T] = c match {
    case a: Array[T] => a
    case b: Buffer[T] => b.toArray
    case l: List[T] => l.toArray
    case l: JList[T] => asScalaBuffer(l).toArray
    case _ => throw new IllegalArgumentException(c.getClass() + " can't be converted to an Array")
  }

  def asSeq[T: ClassTag](c: Any): Seq[T] = c match {
    case a: Array[T] => a
    case b: Buffer[T] => b
    case l: List[T] => l
    case l: JList[T] => asScalaBuffer(l).toSeq
    case _ => throw new IllegalArgumentException(c.getClass() + " can't be converted to a Seq")
  }

}