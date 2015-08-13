package org.graphlab.create

import java.io._
import java.net._
import java.nio.charset.Charset
import java.util.{List => JList, ArrayList => JArrayList, Map => JMap, Collections}


import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.Try
import scala.io.Source

import net.razorvine.pickle.{Pickler, Unpickler}

import org.apache.spark._
import org.apache.spark.api.java.{JavaSparkContext, JavaPairRDD, JavaRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.commons.io.FileUtils

// REQUIRES JAVA 7.  
// Consider switching to http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/IOUtils.html#toByteArray%28java.io.InputStream%29
import java.nio.file.Files
import java.nio.file.Paths

object GraphLabUtil {

  object Mode extends Enumeration {
       type Mode = Value
       val EscapeChar, Normal = Value
  }
  def UnEscapeString(s: String) : String = { 
       val replace_char = '\u001F'
       val output = new StringBuilder()
       import Mode._
       var status = Normal
       for (c: Char <- s) {
         if (c == '\\' && status == Normal) { 
            status = EscapeChar
         }
         else if (status == EscapeChar && c == replace_char) { 
            status = Normal
            output+','
         }
         else if (status == EscapeChar && c == 'n') { 
            status = Normal
            output+'\n'
         }
         else if (status == EscapeChar && c == 'r') { 
            status = Normal
            output+'\r'
         }
         else if (status == EscapeChar && c == 'b') { 
            status = Normal
            output+'\b'
         }
         else if (status == EscapeChar && c == 't') { 
            status = Normal
            output+'\t'
         }
         else if (status == EscapeChar && c == '\"') { 
            status = Normal
            output+'\"'
         }
         else if (status == EscapeChar && c == '\'') { 
            status = Normal
            output+'\''
         }
         else {  
            output+c 
            status = Normal
         }
       } 
       return output.toString
  }
   
  /**
   * Write an integer in native order.
   */
  def writeInt(x: Int, out: java.io.OutputStream) {
    out.write(x.toByte)
    out.write((x >> 8).toByte)
    out.write((x >> 16).toByte)
    out.write((x >> 24).toByte)
  }

  def EscapeString(s: String) : String = { 
    val replace_char = '\u001F'
    val output = new StringBuilder()
    for (c: Char <- s) {
      if (c == '\\') { 
        output+'\\'
        output+c
      }
      else if (c == ',') { 
        output+'\\'
        output+replace_char
      }
      else if (c == '\n') { 
        output+'\\'
        output+'n'
      }
      else if (c == '\b') { 
        output+'\\'
        output+'b'
      }
      else if (c == '\t') { 
        output+'\\'
        output+'t'
      }
      else if (c == '\r') { 
        output+'\\'
        output+'r'
      }
      else if (c == '\'') { 
        output+'\\'
        output+'\''
      }
      else if (c == '\"') { 
        output+'\\'
        output+'\''
      }
      else {  
        output+c 
      }
    } 
    return output.toString
  } 

  class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
    def accept(dir: File, name: String): Boolean = {
      !name.equals(filterName)
    }
  }


  /**
   * Install the bundled binary in the temporary directory location
   */
  def installBinary(name: String) {
    val rootDirectory = SparkFiles.getRootDirectory()
    val outputPath = Paths.get(rootDirectory, name)
    if (!outputPath.toFile().exists()) {
      // Get the binary resources bundled in the jar file
      // Note that the binary must be located in: 
      //   src/main/resources/org/graphlab/create/
      val in = GraphLabUtil.getClass().getResourceAsStream(name)
      Files.copy(in, outputPath)
      outputPath.toFile().setExecutable(true)
    }
  }


  def pipedGLCPartition(command: String,
      iter: Iterator[Array[Byte]], 
      envVars: Map[String, String] = Map(),
      mode: String): 
    Iterator[String] = {
    // Much of this code is "borrowed" from org.apache.spark.rdd.PippedRDD
    val pb = new java.lang.ProcessBuilder(command.split(" ").toList)

    // Add the environmental variables to the process.
    val currentEnvVars = pb.environment()
    envVars.foreach { case (variable, value) => currentEnvVars.put(variable, value) }
    //envVars.foreach {println (_)}
    // Set the working directory 
    pb.directory(new File(SparkFiles.getRootDirectory()))

    // Luanch the graphlab create process
    val proc = pb.start()

    // Start a thread to print the process's stderr to ours
    new Thread("stderr reader for " + command) {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          // scalastyle:off println
          System.err.println(line)
          // scalastyle:on println
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    new Thread("stdin writer for " + command) {
      override def run() {
        if(mode == "tosframe"){
          val out = proc.getOutputStream
          for(bytes <- iter) {
            writeInt(bytes.length, out)
            out.write(bytes)
          }
          // Send end of file
          writeInt(-1, out)
          out.close()
        }
        else {
          val out = new PrintWriter(proc.getOutputStream)
          val unpickle = new Unpickler
          for (elem <- iter) {
            var l: JArrayList[String] = unpickle.loads(elem).asInstanceOf[JArrayList[String]]
            l.foreach(item => out.println(item))
          }
          out.close()
        }
      }
    }.start()

    // Return an iterator that read lines from the process's stdout
    val pathNames = Source.fromInputStream(proc.getInputStream).getLines()

    new Iterator[String] {
      def next(): String = {
        val fileName = pathNames.next()
        /**
         * Consider sending the files directly rather than pulling at the driver
         *
        val fin = new FileInputStream(fileName)
        TODO: switch to apache commons see note on line 21
        val path = Paths.get(fileName);
        val bytes = Files.readAllBytes(path);
        (fileName, bytes)
        */
        fileName
      }
      def hasNext: Boolean = {
        if (pathNames.hasNext) {
          true
        } else {
          val exitStatus = proc.waitFor()
          if (exitStatus != 0) {
            throw new Exception("Subprocess exited with status " + exitStatus)
          }
          false
        }
      }
    }
  }


  /**
   * pipeToSFrames takes a command an array of serialized bytes representing 
   * python objects to be converted into an SFrame.  This function then uses
   * the command on each partition to launch a native GraphLab process which
   * reads the bytes through standard in and outputs SFrames to a shared
   * FileSystems (either HDFS or the local temporary directory).  
   * This function then returns an array of filenames referencing each 
   * of the constructed SFrames.
   */
  def pipeToSFrames(command: String, jrdd: JavaRDD[Array[Byte]],
    envVars: java.util.HashMap[String, String], mode: String): JavaRDD[String] = {
    println("Calling the pipe to SFRame function")
    var files = jrdd.rdd.mapPartitions {
      (iter: Iterator[Array[Byte]]) => pipedGLCPartition(command, iter, envVars.toMap,mode)
    }
    files
  }
  

  def stringToByte(jRDD: JavaRDD[String]): JavaRDD[Array[Byte]] = { 
    jRDD.rdd.mapPartitions { iter =>
      iter.map { row =>
        row match {
            case str:String => new sun.misc.BASE64Decoder().decodeBuffer(str)
          //case str:String => UnEscapeString(str).toCharArray.map(_.toByte)
        } 
      }

    }
  }
 
   
  def byteToString(jRDD: JavaRDD[Array[Byte]]): JavaRDD[String] = { 
    jRDD.rdd.mapPartitions { iter =>
      iter.map { row =>
        row match {
          //case bytes:Array[Byte] => EscapeString(new String(bytes.map(_.toChar)))
           case bytes:Array[Byte] => new sun.misc.BASE64Encoder().encode(bytes).replaceAll("\n","")
        } 
      }

    }
  }
 
  def unEscapeJavaRDD(jRDD: JavaRDD[Array[Byte]]): JavaRDD[Array[Byte]] = {
     //val jRDD = pythonToJava(rdd)
     jRDD.rdd.mapPartitions { iter =>
      val unpickle = new Unpickler
      val pickle = new Pickler
      iter.map { row =>
        row match {
           //case obj: String => obj.split(",").map(UnEscapeString(_))
           case everythingElse: Array[Byte] => pickle.dumps(unpickle.loads(everythingElse) match {
             case str: String => UnEscapeString(str) 
           })
        }
      }
     
     }.toJavaRDD()
  } 
   
 def toJavaStringOfValues(jRDD: RDD[Any]): JavaRDD[String] = {

  jRDD.mapPartitions { iter =>
      iter.map { row =>
        row match {
          case obj: Seq[Any] => obj.map { item => 
            item match {
              case str: String => EscapeString(str)
              case others: Any => others
            }
          }.mkString(",")
        }   
      }   
    }.toJavaRDD()
  }
  def splitUnEscapeJavaRDD(jRDD: JavaRDD[Array[Byte]]): JavaRDD[Array[String]] = {
     jRDD.rdd.mapPartitions { iter =>
      val unpickle = new Unpickler
      val pickle = new Pickler
      iter.map { row =>
        unpickle.loads(row) match {
           //case obj: String => obj.split(",").map(UnEscapeString(_))
           //case everything: Array[Byte] => pickle.dumps(unpickle.loads(everythingElse) match {
             case str: String => str.split(",").map(UnEscapeString(_))
           //}
        }
      }
     }.toJavaRDD()

  }
   
  def pythonToJava(pyRDD: JavaRDD[Any]): JavaRDD[Object] = {
    pyRDD.rdd.mapPartitions { iter =>
      val unpickle = new Unpickler
      iter.map { row =>
        row match { 
          case obj: String => obj
          case everythingElse: Array[Byte] => unpickle.loads(everythingElse)
        }
      }
    }.toJavaRDD()
  }

} // End of GraphLabUtil
