package org.graphlab.create

import java.io.File
import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest._

import scala.collection.mutable.Stack



/**
 * Test the graphlab util functionality
 */
@RunWith(classOf[JUnitRunner])
class GraphLabUtilTestSuite extends FunSuite with BeforeAndAfter {

  var sc: SparkContext = null
  var sqlContext: SQLContext = null


  before {
    println("setup begun")
    val conf = new SparkConf().setAppName("test").setMaster("local")
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
  }

  test("save a dataframe of ints to an sframe") {
    val df = sqlContext.createDataFrame(sc.parallelize(0 to 1000).map(x => (x, x)))
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(df, tmpDir.toString, "ints")
  }

  test("save a dataframe of doubles to an sframe") {
    val df = sqlContext.createDataFrame(sc.parallelize(0 to 1000).map(x => (x, x.toDouble)))
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(df, tmpDir.toString, "ints")
  }

  test("save a dataframe of floats to an sframe") {
    val df = sqlContext.createDataFrame(sc.parallelize(0 to 1000).map(x => (x, x.toDouble, x.toFloat)))
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(df, tmpDir.toString, "ints")
  }

  test("save a dataframe of strings to an sframe") {
    val df = sqlContext.createDataFrame(sc.parallelize(0 to 1000).map(x => (x, x.toDouble, x.toFloat, x.toString)))
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(df, tmpDir.toString, "ints")
  }

  test("save a dataframe of x,y (double array, double) pairs to an sframe") {
    val df = sqlContext.createDataFrame(sc.parallelize(0 to 1000).map(x => (Array(1.0, 2.0, 3.0), 1.0)))
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(df, tmpDir.toString, "ints")
  }



  after {
    sc.stop
  }

}
