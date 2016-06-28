/**
  * *****************************************************************************
  * Copyright (c) 2016 IBM Corp.
  *
  * Created by Basho Technologies for IBM
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  * *****************************************************************************
  */
package com.basho.riak.spark.rdd.japi

import com.basho.riak.spark.japi.rdd.RiakJavaPairRDD
import com.basho.riak.spark.rdd.{RiakCommonTests, RiakRDD}
import com.basho.riak.spark.util.JavaApiHelper._
import org.junit.Test
import org.junit.experimental.categories.Category
import org.mockito.Mockito._
import org.junit.Assert._

@Category(Array(classOf[RiakCommonTests]))
class RiakJavaPairRDDTest {

  @Test
  def createWithClassTags: Unit = {
    val kClassTag = getClassTag(classOf[String])
    val vClassTag = getClassTag(classOf[AnyRef])
    val rdd: RiakRDD[(String, AnyRef)] = mock(classOf[RiakRDD[(String, AnyRef)]])
    val pairRDD: RiakJavaPairRDD[String, AnyRef] = new RiakJavaPairRDD[String, AnyRef](rdd, kClassTag, vClassTag)
    assertEquals(kClassTag, pairRDD.kClassTag)
    assertEquals(vClassTag, pairRDD.vClassTag)
  }

  /*
   * Unit test for verifying logic after removing redundant ClassTag fields for key and value from
   * com.basho.riak.spark.japi.rdd.RiakJavaPairRDD class
   */
  @Test
  def createWithClass: Unit = {
    val rdd: RiakRDD[(String, Object)] = mock(classOf[RiakRDD[(String, Object)]])

    val pairRDD = new RiakJavaPairRDD(rdd, getClassTag(classOf[String]), getClassTag(classOf[Object]))
    assertEquals(getClassTag(classOf[String]), pairRDD.kClassTag)
    assertEquals(getClassTag(classOf[Object]), pairRDD.vClassTag)
  }
}
