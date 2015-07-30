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


  /**
   * Creates a symlink. Note jdk1.7 has Files.createSymbolicLink but not used here
   * for jdk1.6 support.  Supports windows by doing copy, everything else uses "ln -sf".
   * @param src absolute path to the source
   * @param dst relative path for the destination
   */
  // def symlink(src: File, dst: File) {
  //   if (!src.isAbsolute()) {
  //     throw new IOException("Source must be absolute")
  //   }
  //   if (dst.isAbsolute()) {
  //     throw new IOException("Destination must be relative")
  //   }
  //   var cmdSuffix = ""
  //   val linkCmd = if (isWindows) {
  //     // refer to http://technet.microsoft.com/en-us/library/cc771254.aspx
  //     cmdSuffix = " /s /e /k /h /y /i"
  //     "cmd /c xcopy "
  //   } else {
  //     cmdSuffix = ""
  //     "ln -sf "
  //   }
  //   import scala.sys.process._
  //   (linkCmd + src.getAbsolutePath() + " " + dst.getPath() + cmdSuffix) lines_!
  //   ProcessLogger(line => logInfo(line))
  // }

  class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
    def accept(dir: File, name: String): Boolean = {
      !name.equals(filterName)
    }
  }

  def pipedGLCPartition(command: String,
      iter: Iterator[Array[Byte]], 
      envVars: Map[String, String] = Map()): 
    Iterator[String] = {
    // Much of this code is borrowed from org.apache.spark.rdd.PippedRDD
    val pb = new java.lang.ProcessBuilder(command.split(" ").toList)

    // Add the environmental variables to the process.
    val currentEnvVars = pb.environment()
    envVars.foreach { case (variable, value) => currentEnvVars.put(variable, value) }

    // // This code is setup to ensure that each worker thread gets it's own isolated directory
    // val taskDirectory = "tasks" + File.separator + java.util.UUID.randomUUID.toString
    // var workInTaskDirectory = false
    // //val currentDir = new File(".")
    // val currentDir = new File(SparkFiles.getRootDirectory())
    // val taskDirFile = new File(taskDirectory)
    // taskDirFile.mkdirs()
    // // try { // TODO: should come up with a way to manage errors more effectively (log4j?)
    //   val tasksDirFilter = new NotEqualsFileNameFilter("tasks")

    //   // Need to add symlinks to jars, files, and directories.  On Yarn we could have
    //   // directories and other files not known to the SparkContext that were added via the
    //   // Hadoop distributed cache.  We also don't want to symlink to the /tasks directories we
    //   // are creating here.
    //   for (file <- currentDir.list(tasksDirFilter)) {
    //     val fileWithDir = new File(currentDir, file)
    //     val src = new File(fileWithDir.getAbsolutePath()).toPath()
    //     val dst = new File(taskDirectory + File.separator + fileWithDir.getName()).toPath
    //     // TODO: This requires java 7.  Fix Spark implementation of code and put back here.
    //     Files.createSymbolicLink(src, dst)
    //   }
    //   pb.directory(taskDirFile)
    //   workInTaskDirectory = true
    // // } catch {
    // //   case e: Exception => println("Error")
    // // }

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
        val out = proc.getOutputStream
        for(bytes <- iter) {
          writeInt(bytes.length, out)
          out.write(bytes)
        }
        // Send end of file
        writeInt(-1, out)
        out.close()
      }
    }.start()

    // // Wait for GLC to terminate
    // proc.waitFor()

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

          // // cleanup task working directory if used
          // if (workInTaskDirectory) {
          //   scala.util.control.Exception.ignoring(classOf[IOException]) {
          //     FileUtils.deleteDirectory(new File(taskDirectory))
          //     // Utils.deleteRecursively(new File(taskDirectory))
          //   }
          //   //logDebug("Removed task working directory " + taskDirectory)
          // }
          false
        }
      }
    }
  }


  def pipeToSFrames(command: String, jrdd: JavaRDD[Array[Byte]],
    envVars: java.util.HashMap[String, String]): JavaRDD[String] = {
    println("Calling the pipe to SFRame function")
    var files = jrdd.rdd.mapPartitions {
      (iter: Iterator[Array[Byte]]) => pipedGLCPartition(command, iter, envVars.toMap)
    }
    // TODO: Implement reduction tree.
    // var nextSize = files.numPartitions / 2
    // while (nextSize > 1) {
    //   files = files.coalesce(nextSize).mapPartitions(concatSFrames)
    //   nextSize /= 2
    // }
    // for ((fname, bytes) <- files.collect()) {
    //   println(s"Saving bytes locally to $fname")
    //   val fos = new java.io.FileOutputStream(fname)
    //   fos.write(bytes)
    //   fos.close()
    // }
    files
  }

  // def pipeToSFrames(command: String, jrdd: JavaRDD[Array[Byte]],
  //   envVars: java.util.HashMap[String, String]): JavaRDD[String] = {
  //   println("Calling the pipe to SFRame function")
  //   pipeToSFrames(command, jrdd, envVars)
  // }

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

}
