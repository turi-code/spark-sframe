package org.apache.spark.dato

import org.graphlab.create.GraphLabUtil
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.Map
import java.util.HashMap
import java.util.ArrayList
import java.util.{Date,GregorianCalendar}
import java.sql.Date

object EvaluateRDD {
  
  def inferSchema(obj: Any): DataType = {
    if(obj.isInstanceOf[Int]) { 
      IntegerType
    } else if(obj.isInstanceOf[String]) { 
      StringType
    } else if(obj.isInstanceOf[Double]) { 
      DoubleType
    } else if(obj.isInstanceOf[Long]) { 
      LongType
    } else if(obj.isInstanceOf[Float]) { 
      FloatType
    } else if(obj.isInstanceOf[Map[_,_]]) {
      MapType(inferSchema(obj.asInstanceOf[Map[_,_]].head._1),inferSchema(obj.asInstanceOf[Map[_,_]].head._2))
    } else if(obj.isInstanceOf[java.util.HashMap[_,_]]) {
      MapType(inferSchema(obj.asInstanceOf[java.util.HashMap[_,_]].head._1),inferSchema(obj.asInstanceOf[java.util.HashMap[_,_]].head._2))
    } else if(obj.isInstanceOf[Array[_]]) {
      ArrayType(inferSchema(obj.asInstanceOf[Array[_]](0)))
    } else if(obj.isInstanceOf[java.util.ArrayList[_]]) {
      ArrayType(inferSchema(obj.asInstanceOf[java.util.ArrayList[_]](0)))
    } else if(obj.isInstanceOf[java.util.GregorianCalendar]) {
      TimestampType
    } else if(obj.isInstanceOf[java.util.Date] || obj.isInstanceOf[java.sql.Date]) {
      DateType
    } else { 
      StringType
    }
  }

  def toScala(obj: Any): Any = {
    if (obj.isInstanceOf[java.util.HashMap[_,_]]) {
      val jmap = obj.asInstanceOf[java.util.HashMap[_,_]]
      jmap.map { case (k,v) => toScala(k) -> toScala(v) }.toMap
    }
    else if(obj.isInstanceOf[java.util.ArrayList[_]]) {
      val buf = ArrayBuffer[Any]()
      val jArray = obj.asInstanceOf[java.util.ArrayList[_]]
      for(item <- jArray) {
        buf += toScala(item)
      }
      buf.toArray
    } else if(obj.isInstanceOf[java.util.GregorianCalendar]) {
      new java.sql.Timestamp(obj.asInstanceOf[java.util.GregorianCalendar].getTime().getTime())
    } else {
      obj
    }
  }
  def toSparkDataFrame(sqlContext: SQLContext, rdd: RDD[java.util.HashMap[String,_]]): DataFrame = { 
    val scalaRDD = rdd.map(l => toScala(l))
    val rowRDD = scalaRDD.map(l => Row.fromSeq(l.asInstanceOf[Map[_,_]].values.toList))
    
    var sample_data: java.util.HashMap[String,_] = rdd.take(1)(0)
    
    var schema_list: ListBuffer[StructField] = new ListBuffer[StructField]()
    for ((name,v) <- sample_data) { 
      schema_list.append(StructField(name,inferSchema(v)))
    }
    sqlContext.createDataFrame(rowRDD,StructType(schema_list))
  }
}
