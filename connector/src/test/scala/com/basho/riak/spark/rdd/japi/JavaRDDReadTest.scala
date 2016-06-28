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

import com.basho.riak.spark.rdd.{SingleNodeRiakSparkTest, RiakCommonTests}
import org.junit.Test
import org.junit.experimental.categories.Category
import com.basho.riak.spark.japi.SparkJavaUtil._
import org.junit.Assert._

@Category(Array(classOf[RiakCommonTests]))
class JavaRDDReadTest extends SingleNodeRiakSparkTest with AbstractJavaTest {

  protected override val jsonData: Option[String] =
    Some("[" +
      " { key: 'key-1', indexes: {creationNo: 1, category: 'neighbor'}, value: {user_id: 'u1', timestamp: '2014-11-24T13:14:04.823Z'}}" +
      ",{ key: 'key-2', indexes: {creationNo: 2, category: 'visitor'}, value:  {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}}" +
      ",{ key: 'key-3', indexes: {creationNo: 3, category: 'neighbor'}, value: {user_id: 'u1', timestamp: '2014-11-24T13:18:04'}}" +
      ",{ key: 'key-4', indexes: {creationNo: 4, category: 'stranger'}, value: {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}}" +
      ",{ key: 'key-5', indexes: {creationNo: 5, category: 'stranger'}, value: {user_id: 'u3', timestamp: '2014-11-24T13:16:04.823Z'}}" +
      ",{ key: 'key-6', indexes: {creationNo: 6, category: 'stranger'}, value: {user_id: 'u3', timestamp: '2014-11-24T13:21:04.825Z'}}" +
      "]")


  @Test
  def readJSONASString: Unit = {
    val rdd = jsc.riakBucket(DefaultNamespace, classOf[String])
      .query2iRange(CreationIndex, 1L, 4L)

    val results = rdd.takeOrdered(100)
    assertEquals(4, results.size())

    assertEquals("{\n" +
      "  \"user_id\" : \"u1\",\n" +
      "  \"timestamp\" : \"2014-11-24T13:14:04.823Z\"\n" +
      "}", results.get(0))
  }

  @Test
  def readAll: Unit = {
    val rdd = jsc.riakBucket(DefaultNamespace, classOf[String]).queryAll

    val results = rdd.takeOrdered(100)
    assertEquals(6, results.size())
  }
}
