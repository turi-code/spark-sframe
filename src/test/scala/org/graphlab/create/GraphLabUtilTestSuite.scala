/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
package org.graphlab.create

import java.io.File
import java.nio.file.Files
import java.sql.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType,MapType}


import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest._
import Matchers._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import net.razorvine.pickle.custom.{Pickler, Unpickler}

import scala.collection.mutable.Stack



/**
 * Test the graphlab util functionality
 */
@RunWith(classOf[JUnitRunner])
class GraphLabUtilTestSuite extends FunSuite with BeforeAndAfter {

  var sc: SparkContext = null
  var sqlContext: SQLContext = null



  before {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    println("setup begun")
    val conf = new SparkConf().setAppName("test").setMaster("local")
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
  }


  test("save a dataframe of ints to an sframe") {
    val df = sqlContext.createDataFrame(sc.parallelize(0 to 1000).map(x => (x, x)))
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(df, tmpDir.toString, "test")
    val rdd = GraphLabUtil.toRDD(sc, outputFname).cache
    assert(rdd.count === df.count, "rdds are same dimension")
    assert(rdd.take(1)(0).size === 2, "Resulting rdd rows should be two dimensional")
  }


  test("save a dataframe of doubles to an sframe") {
    val df = sqlContext.createDataFrame(sc.parallelize(0 to 1000).map(x => (x, x.toDouble)))
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(df, tmpDir.toString, "test")
    val rdd = GraphLabUtil.toRDD(sc, outputFname).cache
    assert(rdd.count === df.count, "rdds are same dimension")
    assert(rdd.take(1)(0).size === 2, "Resulting rdd rows should be two dimensional")
  }


  test("save a dataframe of floats to an sframe") {
    val df = sqlContext.createDataFrame(sc.parallelize(0 to 1000).map(x => (x, x.toDouble, x.toFloat)))
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(df, tmpDir.toString, "test")
    val rdd = GraphLabUtil.toRDD(sc, outputFname).cache
    assert(rdd.count === df.count, "rdds are same dimension")
    assert(rdd.take(1)(0).size === 3, "Resulting rdd rows should be two dimensional")
  }


  test("save a dataframe of strings to an sframe") {
    val df = sqlContext.createDataFrame(sc.parallelize(0 to 1000).map(x => (x, x.toDouble, x.toFloat, x.toString)))
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(df, tmpDir.toString, "test")
    val rdd = GraphLabUtil.toRDD(sc, outputFname).cache
    assert(rdd.count === df.count, "rdds are same dimension")
    assert(rdd.take(1)(0).size === 4, "Resulting rdd rows should be two dimensional")
  }


  test("save a dataframe of x,y (double array, double) pairs to an sframe") {
    val df = sqlContext.createDataFrame(sc.parallelize(0 to 1000).map(x => (Array(1.0, 2.0, 3.0), 1.0)))
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(df, tmpDir.toString, "test")
    val rdd = GraphLabUtil.toRDD(sc, outputFname).cache
    assert(rdd.count === df.count, "rdds are same dimension")
    assert(rdd.take(1)(0).size === 2, "Resulting rdd rows should be two dimensional")
    rdd.take(1)(0).get("_1").asInstanceOf[Array[Double]] should equal (Array(1.0, 2.0, 3.0))
  }

  
  test("save a dataframe of x,y (double array, double, datetime) pairs to an sframe") {
    val df = sqlContext.createDataFrame(sc.parallelize(0 to 1000)
      .map(x => (Array(1.0, 2.0, 3.0), 1.0, new Date(0))))
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(df, tmpDir.toString, "test")
    val rdd = GraphLabUtil.toRDD(sc, outputFname).cache
    assert(rdd.count === df.count, "rdds are same dimension")
    assert(rdd.take(1)(0).size === 3, "Resulting rdd rows should be two dimensional")
    rdd.take(1)(0).get("_1").asInstanceOf[Array[Double]] should equal (Array(1.0, 2.0, 3.0))
  }

  test("save a dataframe of MapType to an sframe") {
    val schema = StructType(Seq(StructField("attributes", MapType(StringType, StringType, false))))
    var A: Map[String,String] = Map()
    A += ( "J" -> "J")
    A += ( "F" -> "F")
    var rdd = sc.parallelize(Array(A))
    val row_rdd = rdd.map(p => Row(p))
    val df = sqlContext.createDataFrame(row_rdd,schema)
    val tmpDir = Files.createTempDirectory("sframe_test")
    val sframeFileName = GraphLabUtil.toSFrame(df, tmpDir.toString, "test")
    val output_rdd = GraphLabUtil.toRDD(sc, sframeFileName).cache
    assert(rdd.count === output_rdd.count, "rdds are same dimension") 
    val h = output_rdd.collect()(0)
    assert(rdd.collect()(0)("F") == h.get("attributes").asInstanceOf[java.util.HashMap[String,String]].get("F"))
    assert(rdd.collect()(0)("J") == h.get("attributes").asInstanceOf[java.util.HashMap[String,String]].get("J"))
  }
  
  test("Checking fields names") {
    val x = sc.parallelize(0 to 1000).map(x => (x.toString, x.toDouble))
    val schema = StructType(Array(StructField("name", StringType), StructField("age", DoubleType)))
    val df = sqlContext.createDataFrame(x.map(x => Row(x._1, x._2)), schema)
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(df, tmpDir.toString, "test")
    val rdd = GraphLabUtil.toRDD(sc, outputFname).cache
    assert(rdd.count === df.count, "rdds are same dimension")
    assert(rdd.take(1)(0).size === 2, "Resulting rdd rows should be two dimensional")
    assert(rdd.take(1)(0).containsKey("name"), "RDD does not have nave field")
    assert(rdd.take(1)(0).containsKey("age"), "RDD does not have age field")
    val values = rdd.collect()
    for (m <- values) {
      assert(m.get("name").asInstanceOf[String].toInt === m.get("age").asInstanceOf[Double].toInt,
        "Name not equal to age")
    }
  }


  test("Checking RDD of strings") {
    val x = sc.parallelize(0 to 1000).map(x => x.toString)
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(x, tmpDir.toString, "test")
    val rdd = GraphLabUtil.toRDD(sc, outputFname).cache
    assert(rdd.count === x.count, "rdds are same dimension")
  }



  after {
    sc.stop
  }

}
