/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.dato

import java.io.OutputStream
import java.sql.Date
import java.util.Date
import java.util.Calendar
import java.util.GregorianCalendar;
import java.util.List
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import net.razorvine.pickle.{IObjectPickler, Opcodes, Pickler}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.collection.mutable
import scala.collection.JavaConversions._

/**
 * Most of the content in this class is borrowed directly from Apache Spark EvaluatePython.scala (thanks!)
 */



/**
 * This function uses the razorvine Pickler to convert an Iterator over
 * java objects to an Iterator over pickled python objects.
 */
class AutoBatchedPickler(iter: Iterator[Any]) extends Iterator[Array[Byte]] {
  EvaluatePython.registerPicklers()
  private val pickle = new Pickler()
  
  private var batch = 1
  private val buffer = new mutable.ArrayBuffer[Any]

  override def hasNext: Boolean = iter.hasNext

  override def next(): Array[Byte] = {
    while (iter.hasNext && buffer.length < batch) {
      buffer += iter.next()
    }
    val bytes = pickle.dumps(buffer.toArray)
    val size = bytes.length
    // let  1M < size < 10M
    if (size < 1024 * 1024) {
      batch *= 2
    } else if (size > 1024 * 1024 * 10 && batch > 1) {
      batch /= 2
    }
    buffer.clear()
    bytes
  }
}

object EvaluatePython {
  /**
   * Helper for converting from Catalyst type to java type suitable for Pyrolite.
   */
  def toJava(obj: Any, dataType: DataType): Any = (obj, dataType) match {
    case (null, _) => null
    case (a: Any, struct: StructType) =>
       val row = a.asInstanceOf[Row]
       val jmap = new java.util.HashMap[String, Any](row.length) 
       for( i <- 0 until row.length){
         jmap.put(struct.fieldNames(i),toJava(row.get(i),struct.fields(i).dataType))
       }
       jmap 
    case (a: Any, array: ArrayType) =>
      if (a.isInstanceOf[ArrayBuffer[_]]){
        val c : java.util.List[Any] = a.asInstanceOf[ArrayBuffer[_]]
        c
      } 
      else if (a.isInstanceOf[collection.mutable.WrappedArray[_]]) {
        val c = a.asInstanceOf[collection.mutable.WrappedArray[_]].toArray
        c
      } else 
        a
    case (o:Any, mt: DateType) =>
      new java.util.Date(o.asInstanceOf[java.sql.Date].getDate())
    case (o: Any, mt: MapType) =>
      val map = o.asInstanceOf[Map[_,_]]
      val jmap = new java.util.HashMap[Any, Any](map.size)
      map.foreach( p => 
        jmap.put(toJava(p._1, mt.keyType), toJava(p._2, mt.valueType)))
      jmap

    case (ud, udt: UserDefinedType[_]) => toJava(ud, udt.sqlType)

    case (d: Decimal, _) => d.toJavaBigDecimal

    case (s: Any, StringType) => s.toString

    case (other, _) => 
      other
  }

  private val module = "pyspark.sql.types"

  
  /**
   * Pickler for java.util.date. We need to register a customized 
   * pickler for DateType object. The default pickler in net.razorvine.pickle
   * is very broken! 
   */

  private class DateTypePickler extends IObjectPickler {
    private val cls = classOf[java.util.Date]

    def register(): Unit = {
      Pickler.registerCustomPickler(cls, this)  
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      val date = obj.asInstanceOf[java.util.Date]
      val cal= new GregorianCalendar()
      cal.setTime(date)
      out.write(Opcodes.GLOBAL);
      out.write("datetime\ndatetime\n".getBytes());
      out.write(Opcodes.MARK);
      pickler.save(cal.get(Calendar.YEAR));
      pickler.save(cal.get(Calendar.MONTH)+1);    // months start at 0 in java
      pickler.save(cal.get(Calendar.DAY_OF_MONTH));
      pickler.save(cal.get(Calendar.HOUR_OF_DAY));
      pickler.save(cal.get(Calendar.MINUTE));
      pickler.save(cal.get(Calendar.SECOND));
      pickler.save(cal.get(Calendar.MILLISECOND)*1000);
      out.write(Opcodes.TUPLE);
      out.write(Opcodes.REDUCE);
    }
  }
  /**
   * Pickler for StructType
   */
  private class StructTypePickler extends IObjectPickler {

    private val cls = classOf[StructType]

    def register(): Unit = {
      Pickler.registerCustomPickler(cls, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      out.write(Opcodes.GLOBAL)
      out.write((module + "\n" + "_parse_datatype_json_string" + "\n").getBytes("utf-8"))
      val schema = obj.asInstanceOf[StructType]
      pickler.save(schema.json)
      out.write(Opcodes.TUPLE1)
      out.write(Opcodes.REDUCE)
    }
  }

  private[this] var registered = false

  /**
    * This should be called before trying to serialize any above classes un cluster mode,
    * this should be put in the closure
    */
  def registerPicklers(): Unit = {
    synchronized {
      if (!registered) {
        new StructTypePickler().register()
        new DateTypePickler().register()
        registered = true
      }
    }
  }

}