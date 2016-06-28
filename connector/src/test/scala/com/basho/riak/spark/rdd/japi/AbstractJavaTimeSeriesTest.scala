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

import com.basho.riak.spark.rdd.timeseries.AbstractTimeSeriesTest
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext

abstract class AbstractJavaTimeSeriesTest(createTestDate: Boolean) extends AbstractTimeSeriesTest {
  protected var jsc: JavaSparkContext = _

  override def createSparkContext(conf: SparkConf): SparkContext = {
    val sc: SparkContext = new SparkContext(conf)
    jsc = new JavaSparkContext(sc)
    sc
  }
}


