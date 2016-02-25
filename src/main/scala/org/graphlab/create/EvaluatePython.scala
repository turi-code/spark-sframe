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
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import net.razorvine.pickle.{IObjectPickler, Opcodes, Pickler}

import org.apache.spark.sql.catalyst.InternalRow

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.mutable
import scala.collection.JavaConversions._



/**
 * A row implementation that uses an array of objects as the underlying storage.  Note that, while
 * the array is not copied, and thus could technically be mutated after creation, this is not
 * allowed.
 */
class GenericRow(val values: Array[Any]) extends Row {
  /** No-arg constructor for serialization. */
  protected def this() = this(null)

  def this(size: Int) = this(new Array[Any](size))

  override def length: Int = values.length

  override def get(i: Int): Any = values(i)

  override def toSeq: Seq[Any] = values.clone()

  override def copy(): GenericRow = this
}

class GenericRowWithSchema(values: Array[Any], override val schema: StructType)
  extends GenericRow(values) {

  /** No-arg constructor for serialization. */
  protected def this() = this(null, null)

  override def fieldIndex(name: String): Int = schema.fieldIndex(name)
}

/**
 * This function is borrowed directly from Apache Spark SerDeUtil.scala (thanks!)
 * 
 * It uses the razorvine Pickler (again thank you!) to convert an Iterator over
 * java objects to an Iterator over pickled python objects.
 *
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

    case (row: InternalRow, struct: StructType) =>
      val values = new Array[Any](row.numFields)
      var i = 0
      while (i < row.numFields) {
        values(i) = toJava(row.get(i, struct.fields(i).dataType), struct.fields(i).dataType)
        i += 1
      }
      new GenericRowWithSchema(values, struct)

    case (a: Any, struct: StructType) =>
       val row = a.asInstanceOf[Row]
       val jmap = new java.util.HashMap[String, Any](row.length) 
       for( i <- 0 until row.length){
         jmap.put(struct.fieldNames(i),toJava(row.get(i),struct.fields(i).dataType))
       }
       jmap 
    case (a: Any, array: ArrayType) =>
      if (a.isInstanceOf[collection.mutable.WrappedArray[_]]) {
          a.asInstanceOf[collection.mutable.WrappedArray[_]].toArray
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

    case (s: UTF8String, StringType) => s.toString

    case (other, _) => 
      //println("I am in other")
      other
  }

  private val module = "pyspark.sql.types"

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

  /**
   * Pickler for external row.
   */
  private class RowPickler extends IObjectPickler {

    private val cls = classOf[GenericRowWithSchema]

    // register this to Pickler and Unpickler
    def register(): Unit = {
      Pickler.registerCustomPickler(this.getClass, this)
      Pickler.registerCustomPickler(cls, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      if (obj == this) {
        out.write(Opcodes.GLOBAL)
        out.write((module + "\n" + "_create_row_inbound_converter" + "\n").getBytes("utf-8"))
      } else {
        // it will be memorized by Pickler to save some bytes
        pickler.save(this)
        val row = obj.asInstanceOf[GenericRowWithSchema]
        // schema should always be same object for memoization
        pickler.save(row.schema)
        out.write(Opcodes.TUPLE1)
        out.write(Opcodes.REDUCE)

        out.write(Opcodes.MARK)
        var i = 0
        while (i < row.values.length) {
          pickler.save(row.values(i))
          i += 1
        }
        out.write(Opcodes.TUPLE)
        out.write(Opcodes.REDUCE)
      }
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
        new RowPickler().register()
        registered = true
      }
    }
  }

  /**
   * Convert an RDD of Java objects to an RDD of serialized Python objects, that is usable by
   * PySpark.
   */
  def javaToPython(rdd: RDD[Any]): RDD[Array[Byte]] = {
    rdd.mapPartitions { iter =>
      registerPicklers()  // let it called in executor
      new AutoBatchedPickler(iter)
    }
  }
}