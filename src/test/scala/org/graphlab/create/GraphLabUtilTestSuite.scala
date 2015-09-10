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
    val df = sqlContext.createDataFrame(sc.parallelize(0 to 1000).map(x => (x,x)))
    val tmpDir = Files.createTempDirectory("sframe_test")
    val outputFname = GraphLabUtil.toSFrame(df, tmpDir.toString, "ints")
//    val inrdd = GraphLabUtil.toRDD(sc, outputFname, 2)
//    assertResult(df.count()) {
//      inrdd.count()
//    }
  }

  test("pop is invoked on an empty stack") {

    val emptyStack = new Stack[Int]
    intercept[NoSuchElementException] {
      emptyStack.pop()
    }
    assert(emptyStack.isEmpty)
  }

  after {
    sc.stop
  }

}
