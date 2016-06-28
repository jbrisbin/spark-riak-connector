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

import com.basho.riak.spark.japi.rdd.RiakTSJavaRDD
import com.basho.riak.spark.rdd.timeseries.TimeSeriesData
import com.basho.riak.spark.rdd.{AbstractRDDTest, RiakTSTests}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions, DataFrame, SQLContext, Row}
import org.junit.Test
import org.junit.experimental.categories.Category
import com.basho.riak.spark.japi.SparkJavaUtil._

import scala.collection.JavaConversions._

@Category(Array(classOf[RiakTSTests]))
class TimeSeriesJavaReadTest extends AbstractJavaTimeSeriesTest(false) with AbstractRDDTest {

  @Test
  def readDataAsSqlRowJava: Unit = {
    val rows: RiakTSJavaRDD[Row] = jsc.riakTSTable(bucketName, classOf[Row])
      .sql(s"SELECT user_id, temperature_k FROM $bucketName $sqlWhereClause")

    val data = rows.collect().map(_.toSeq)

    assertEqualsUsingJSONIgnoreOrder("[" +
      "['bryce',305.37]," +
      "['bryce',300.12]," +
      "['bryce',295.95]," +
      "['ratman',362.121]," +
      "['ratman',3502.212]]", data)
  }

  @Test
  def riakTSRDDToDataFrameJava: Unit = {
    val sqlContext: SQLContext = new SQLContext(jsc)
    import sqlContext.implicits._

    val df = jsc.riakTSTable[Row](bucketName, classOf[Row])
      .sql(s"SELECT time, user_id, temperature_k FROM $bucketName $sqlWhereClause")
      .collect()
      .map(r => TimeSeriesData(r.getTimestamp(0).getTime, r.getString(1), r.getDouble(2)))
      .toDF()

    df.registerTempTable("test")

    val data: Array[String] = sqlContext.sql("select * from test").toJSON.collect
    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   {time: 111111, user_id:'bryce', temperature_k:305.37},
        |   {time: 111222, user_id:'bryce', temperature_k:300.12},
        |   {time: 111333, user_id:'bryce', temperature_k:295.95},
        |   {time: 111444, user_id:'ratman', temperature_k:362.121},
        |   {time: 111555, user_id:'ratman', temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }

  @Test
  def riakTSRDDToDataFrameConvertTimestampJava: Unit = {
    val sqlContext: SQLContext = new SQLContext(jsc)
    import sqlContext.implicits._

    val schema = StructType(List(
      StructField(name = "time", dataType = LongType, nullable = true),
      StructField(name = "user_id", dataType = StringType, nullable = true),
      StructField(name = "temperature_k", dataType = DoubleType, nullable = true))
    )

    val df = jsc.riakTSTable(bucketName, schema, classOf[Row])
      .sql(s"SELECT time, user_id, temperature_k FROM $bucketName $sqlWhereClause")
      .collect()
      .map(r => TimeSeriesData(r.getLong(0), r.getString(1), r.getDouble(2)))
      .toDF()

    df.registerTempTable("test")
    val data: Array[String] = sqlContext.sql("select * from test").toJSON.collect
    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   {time: 111111, user_id:'bryce', temperature_k:305.37},
        |   {time: 111222, user_id:'bryce', temperature_k:300.12},
        |   {time: 111333, user_id:'bryce', temperature_k:295.95},
        |   {time: 111444, user_id:'ratman', temperature_k:362.121},
        |   {time: 111555, user_id:'ratman', temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }

  @Test
  def dataFrameGenericLoadJava: Unit = {
    val sqlContext: SQLContext = new SQLContext(jsc)

    sqlContext.udf.register("getMillis", getMillis)
    var df: DataFrame = sqlContext.read.format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >= CAST('$fromStr' AS TIMESTAMP) AND time <= CAST('$toStr' AS TIMESTAMP) AND surrogate_key = 1 AND family = 'f'")

    df = df.select(functions.callUDF("getMillis", df.col("time")).as("time"),
      df.col("family"), df.col("surrogate_key"), df.col("user_id"), df.col("temperature_k"))

    val data = df.toJSON.collect
    assertEqualsUsingJSONIgnoreOrder(
      """[
        |   {surrogate_key:1, family: 'f', time: 111111, user_id:'bryce', temperature_k:305.37},
        |   {surrogate_key:1, family: 'f', time: 111222, user_id:'bryce', temperature_k:300.12},
        |   {surrogate_key:1, family: 'f', time: 111333, user_id:'bryce', temperature_k:295.95},
        |   {surrogate_key:1, family: 'f', time: 111444, user_id:'ratman', temperature_k:362.121},
        |   {surrogate_key:1, family: 'f', time: 111555, user_id:'ratman', temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }

  @Test
  def dataFrameReadShouldConvertTimestampToLongJava: Unit = {
    val sqlContext: SQLContext = new SQLContext(jsc)

    val newSchema = StructType(List(
      StructField(name = "surrogate_key", dataType = LongType),
      StructField(name = "family", dataType = StringType),
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    )

    var df: DataFrame = sqlContext.read.option("spark.riak.partitioning.ts-range-field-name", "time")
      .format("org.apache.spark.sql.riak")
      .schema(newSchema)
      .load(bucketName)
      .filter(s"time >= $queryFromMillis AND time <= $queryToMillis AND surrogate_key = 1 AND family = 'f'")

    df = df.select(df.col("time"), df.col("family"), df.col("surrogate_key"), df.col("user_id"), df.col("temperature_k"))
    val data: Array[String] = df.toJSON.collect

    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   {surrogate_key:1, family: 'f', time: 111111, user_id:'bryce', temperature_k:305.37},
        |   {surrogate_key:1, family: 'f', time: 111222, user_id:'bryce', temperature_k:300.12},
        |   {surrogate_key:1, family: 'f', time: 111333, user_id:'bryce', temperature_k:295.95},
        |   {surrogate_key:1, family: 'f', time: 111444, user_id:'ratman', temperature_k:362.121},
        |   {surrogate_key:1, family: 'f', time: 111555, user_id:'ratman', temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }

  @Test
  def dataFrameReadShouldHandleTimestampAsLongJava: Unit = {
    val sqlContext: SQLContext = new SQLContext(jsc)

    var df: DataFrame = sqlContext.read.format("org.apache.spark.sql.riak")
      .option("spark.riakts.bindings.timestamp", "useLong")
      .option("spark.riak.partitioning.ts-range-field-name", "time")
      .load(bucketName)
      .filter(s"time > $queryFromMillis AND time < $queryToMillis AND surrogate_key = 1 AND family = 'f'")

    df = df.select(df.col("time"), df.col("family"), df.col("surrogate_key"), df.col("user_id"), df.col("temperature_k"))
    val data: Array[String] = df.toJSON.collect

    assertEqualsUsingJSONIgnoreOrder(
      """[
        |   {surrogate_key:1, family: 'f', time: 111111, user_id:'bryce', temperature_k:305.37},
        |   {surrogate_key:1, family: 'f', time: 111222, user_id:'bryce', temperature_k:300.12},
        |   {surrogate_key:1, family: 'f', time: 111333, user_id:'bryce', temperature_k:295.95},
        |   {surrogate_key:1, family: 'f', time: 111444, user_id:'ratman', temperature_k:362.121},
        |   {surrogate_key:1, family: 'f', time: 111555, user_id:'ratman', temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }
}
