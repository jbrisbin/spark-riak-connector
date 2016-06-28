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

import java.util

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.annotations.{RiakIndex, RiakKey}
import com.basho.riak.client.core.query.{RiakObject, Location, Namespace}
import com.basho.riak.spark.rdd.{SingleNodeRiakSparkTest, RiakCommonTests}
import com.basho.riak.spark.writer.mapper.TupleWriteDataMapper
import org.junit.Assert._
import com.basho.riak.spark.japi.SparkJavaUtil._
import org.junit.Test
import org.junit.experimental.categories.Category

import scala.annotation.meta.field

case class ORMDomainObject(
                            @(RiakKey@field)
                            user_id: String,

                            @(RiakIndex@field) (name = "groupId")
                            group_id: Long,

                            login: String)

@Category(Array(classOf[RiakCommonTests]))
class JavaRDDWriteTest extends SingleNodeRiakSparkTest with AbstractJavaTest {

  @Test
  def saveToRiakWithORM: Unit = {
    val objs = util.Arrays.asList(
      ORMDomainObject("user-1", 1, "user1"),
      ORMDomainObject("user-2", 2, "user2"),
      ORMDomainObject("user-3", 3, "user3"))

    val rdd = jsc.parallelize(objs)
    rdd.saveToRiak(DefaultNamespace4Store)

    val data = jsc.riakBucket(DefaultNamespace4Store, classOf[ORMDomainObject])
      .queryBucketKeys("user-1", "user-2", "user-3").collect

    assertEqualsUsingJSONIgnoreOrder(
      """[
        | {login:'user1', group_id:1, user_id:'user-1'},
        | {login:'user2', group_id:2, user_id:'user-2'},
        | {login:'user3', group_id:3, user_id:'user-3'}
        | ]""".stripMargin, data)
  }

  @Test
  def saveToRiakTuple1: Unit = {
    jsc.parallelize(util.Arrays.asList(
      Tuple1("key1"),
      Tuple1("key2"),
      Tuple1("key3")), 1)
      .saveToRiak(DefaultNamespace4Store, TupleWriteDataMapper.factory[Tuple1[String]])

    val t1Data = fetchAllFromBucket(DefaultNamespace4Store)

    // Keys should be generated on the Riak side, therefore they will be ignored
    assertEqualsUsingJSONIgnoreOrder(
      """[
        | ['${json-unit.ignore}', 'key1'],
        | ['${json-unit.ignore}', 'key2'],
        | ['${json-unit.ignore}', 'key3']
        | ]""".stripMargin, t1Data)

    verifyContentTypeEntireTheBucket("text/plain", DefaultNamespace4Store)

  }

  @Test
  def saveToRiakTuple2: Unit = {
    jsc.parallelize(util.Arrays.asList(Tuple2("key1", 1), Tuple2("key2", 2), Tuple2("key3", 3)), 1)
    .saveToRiak(DefaultNamespace4Store, TupleWriteDataMapper.factory[(String, Int)])

    val t2Data = jsc.riakBucket(DefaultNamespace4Store, classOf[Integer]).queryBucketKeys("key1", "key2", "key3").collect
    assertEquals(3, t2Data.size)
    assertEqualsUsingJSONIgnoreOrder("[1,2,3]", t2Data)
  }

  @Test
  def saveToRiakTuple3: Unit = {
    jsc.parallelize(util.Arrays.asList(
      Tuple3("key1", 1, 11),
      Tuple3("key2", 2, 22),
      Tuple3("key3", 3, 33)), 1)
      .saveToRiak(DefaultNamespace4Store, TupleWriteDataMapper.factory[(String, Int, Int)])
    val data = jsc.riakBucket(DefaultNamespace4Store, classOf[util.List[_]]).queryBucketKeys("key1", "key2", "key3").collect
    assertEqualsUsingJSONIgnoreOrder("[" + "[1,11]," + "[2,22]," + "[3,33]" + "]", data)
  }

  private def verifyContentTypeEntireTheBucket(expected: String, ns: Namespace = DEFAULT_NAMESPACE_4STORE): Unit = {
    withRiakDo(session =>{
      foreachKeyInBucket(session, ns, (client:RiakClient, l:Location) => {
        val ro = readByLocation[RiakObject](session, l)
        assertEquals(s"Unexpected RiakObject.contentType\nExpected\t:$expected\nActual\t:${ro.getContentType}", expected, ro.getContentType)
      })
    })
  }
}
