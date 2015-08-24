package org.graphlab.create

import java.io._
import java.net._
import java.nio.charset.Charset
import java.util.{List => JList, ArrayList => JArrayList, Map => JMap, Collections}


import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.Try
import scala.io.Source
import scala.collection.mutable

import net.razorvine.pickle.{Pickler, Unpickler}


import org.apache.spark._
import org.apache.spark.api.java.{JavaSparkContext, JavaPairRDD, JavaRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import org.apache.hadoop.fs.{FileSystem, Path}


// REQUIRES JAVA 7.  
// Consider switching to http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/IOUtils.html#toByteArray%28java.io.InputStream%29
import java.nio.file.Files
import java.nio.file.Paths

object GraphLabUtil {

  /**
   * The types of unity mode supported and their corresponding
   * command line arguments
   */
  object UnityMode extends Enumeration {
    type UnityMode = Value
    val ToSFrame = Value(" --mode=tosframe ")
    val FromSFrame = Value(" --mode=tordd ")
    val Concat = Value(" --mode=concat ")
  }
  import UnityMode._
  

  /**
   * This function is borrowed directly from Apache Spark SerDeUtil.scala (thanks!)
   * 
   * It uses the razorvine Pickler (again thank you!) to convert an Iterator over
   * java objects to an Iterator over pickled python objects.
   *
   */
  class AutoBatchedPickler(iter: Iterator[Any]) extends Iterator[Array[Byte]] {
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

  /**
   * Create the desired output directory which may (is likely)
   * on HDFS.  
   *
   * This funciton recursively creates the entire path:
   *  mkdir -p /way/too/long/path/name
   */
  def makeDir(outputDir: Path, sc: SparkContext): String = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val success = fs.mkdirs(outputDir)
    // TODO: Do something about when not success.
    outputDir.toString
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


  /**
   * Install the bundled binary in this jar stored in:
   *     src/main/deps/org/graphlab/create 
   * in the temporary directory location used by Spark.
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


  /**
   * Get the platform information as a simple string: mac, linux, windows
   */
  def getPlatform(): String = {
    val osName = System.getProperty("os.name")
    if (osName == "Mac OS X") {
      "mac"
    } else if (osName.toLowerCase().contains("windows")) {
      "windows"
    } else if (osName == "Linux") {
      "linux"
    } else {
      throw new Exception("Unsupported platform for Spark Integration.")
    }
  }


  /**
   * Get the platform specific binary name for the sframe binary.
   * Here we assume the binaries have the following names:
   *
   *   -- Mac: spark_unity_mac
   *   -- Linux: spark_unity_linux
   *   -- Windows?: spark_unity_windows
   *
   */
  def getBinaryName(): String =  {
    "spark_unity_" + getPlatform() 
  }


  /**
   * Install all platform specific binaries in the temporary 
   * working directory.
   */
  def installPlatformBinaries() {
    // TODO: Install support dynamic libraries
    installBinary(getBinaryName())
  }


  /**
   * Build and launch a process with the appropriate arguments.
   *
   * TODO: annotate the return type
   */
  def launchProcess(mode: UnityMode, args: List[String]) = {
    // Much of this code is "borrowed" from org.apache.spark.rdd.PippedRDD
    // Construct the process builder with the full argument list 
    // including the unity binary name and unity mode
    val fullArgList = List(getBinaryName(), mode.toString) ++ args
    val pb = new java.lang.ProcessBuilder(fullArgList)
    // Add the environmental variables to the process.
    val env = pb.environment()
    // Getting the current python path and adding a separator if necessary
    // TODO: Make sure the separator is cross platform.
    val addPyPath = "__spark__.jar"
    val pythonPath = 
      if (env.contains("PYTHONPATH")) { 
        env.get("PYTHONPATH") + ":" + addPyPath 
      } else { 
        addPyPath 
      }
    // TODO: verify the python path does not need additional arguments
    env.put("PYTHONPATH", pythonPath)
    // Set the working directory 
    pb.directory(new File(SparkFiles.getRootDirectory()))
    // Luanch the graphlab create process
    val proc = pb.start()
    // Start a thread to print the process's stderr to ours
    new Thread("GraphLab Unity Standard Error Reader") {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()
    proc
  }


  /**
   * This function takes an iterator over arrays of bytes and executes
   * the unity binary
   */
  def toSFrameIterator(
      args: List[String],
      iter: Iterator[Array[Byte]]): 
    Iterator[String] = {
    // Launch the unity process
    val proc = launchProcess(UnityMode.ToSFrame, args)
    // Start a thread to feed the process input from our parent's iterator
    new Thread("GraphLab Unity toSFrame writer") {
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

    // Return an iterator that read lines from the process's stdout
    val pathNames = Source.fromInputStream(proc.getInputStream).getLines()

    new Iterator[String] {
      def next(): String = {
        pathNames.next()
      }
      def hasNext: Boolean = {
        if (pathNames.hasNext) {
          true
        } else {
          val exitStatus = proc.waitFor()
          if (exitStatus != 0) {
            throw new Exception("GraphLab Unity toSFrame processes exit status " + exitStatus)
          }
          false
        }
      }
    }
  }


  /**
   * This function takes a collection of sframes and concatenates
   * them into a single sframe
   *
   * TODO: make this function private. 
   */
  def concat(args: List[String], sframes: Array[String]): String = {
    val proc = launchProcess(UnityMode.Concat, args)

    // Write all the filenames to standard in for the child process
    val out = new PrintWriter(proc.getOutputStream)
    sframes.foreach(out.println(_))
    out.close()

    // wait for the child process to terminate
    val exitStatus = proc.waitFor()
    if (exitStatus != 0) {
      throw new Exception("Subprocess exited with status " + exitStatus)
    }

    // Get an iterator over the output
    val outputIter = Source.fromInputStream(proc.getInputStream).getLines()
    if (!outputIter.hasNext()) {
      throw new Exception("Concatenation failed!")
    }
    // Get and return the name of the final SFrame
    val finalSFrameName = outputIter.next()
    finalSFrameName
  }


  /**
   * This function takes a pyspark RDD exposed by the JavaRDD of bytes
   * and constructs an SFrame.
   *
   * @arg outputDir: The directory in which to store the SFrame
   * @arg args: The list of command line arguments to the unity process
   * @arg jrdd: The java rdd corresponding to the pyspark rdd
   *
   * @return the final filename of the output sframe.
   */
  def pySparkToSFrame(outputDir: String, args: String, jrdd: JavaRDD[Array[Byte]]): String = {
    // Create folders
    val internalOutput: String =
      makeDir(new org.apache.hadoop.fs.Path(outputDir, "internal"), jrdd.sparkContext)
    val argList = args.split(" ").toList
    // pipe to Unity 
    val fnames = jrdd.rdd.mapPartitions (
      (iter: Iterator[Array[Byte]]) => { toSFrameIterator(argList, iter) }
    ).collect()
    val sframe_name = concat(argList, fnames)
    return sframe_name
  }


  /**
   * Take a dataframe and convert it to an sframe
   */
  def toSFrame(df: DataFrame, outputDir: String, prefix: String): String = {
    // Convert the dataframe into a Java rdd of pickles of batches of Rows
    var javaRDDofPickles = df.rdd.mapPartitions {
      (iter: Iterator[Row]) => new AutoBatchedPickler(iter)
    }.toJavaRDD()
    // Construct the arguments to the graphlab unity process
    val args = s"--outputDir=${outputDir} --prefix=${prefix} " +
      "--encoding=batch --type=schemardd"
    pySparkToSFrame(outputDir, args, javaRDDofPickles)
  }
  
   
  // def pythonToJava(pyRDD: JavaRDD[Any]): JavaRDD[Object] = {
  //   pyRDD.rdd.mapPartitions { iter =>
  //     val unpickle = new Unpickler
  //     iter.map { row =>
  //       row match { 
  //         case obj: String => obj
  //         case everythingElse: Array[Byte] => unpickle.loads(everythingElse)
  //       }
  //     }
  //   }.toJavaRDD()
  // }

} // End of GraphLabUtil



////// Old Code


  // /**
  //  * pipeToSFrames takes a command an array of serialized bytes representing 
  //  * python objects to be converted into an SFrame.  This function then uses
  //  * the command on each partition to launch a native GraphLab process which
  //  * reads the bytes through standard in and outputs SFrames to a shared
  //  * FileSystems (either HDFS or the local temporary directory).  
  //  * This function then returns an array of filenames referencing each 
  //  * of the constructed SFrames.
  //  */
  // def unityPipe(mode: UnityMode, args: List[String], jrdd: JavaRDD[Array[Byte]]): 
  // JavaRDD[String] = {
  //   val files = jrdd.rdd.mapPartitions {
  //     (iter: Iterator[Array[Byte]]) => unityIterator(mode, args, iter, envVars.toMap)
  //   }
  //   files
  // }


  // class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
  //   def accept(dir: File, name: String): Boolean = {
  //     !name.equals(filterName)
  //   }
  // }


// def EscapeString(s: String) : String = { 
//     val replace_char = '\u001F'
//     val output = new StringBuilder()
//     for (c: Char <- s) {
//       if (c == '\\') { 
//         output+'\\'
//         output+c
//       }
//       else if (c == ',') { 
//         output+'\\'
//         output+replace_char
//       }
//       else if (c == '\n') { 
//         output+'\\'
//         output+'n'
//       }
//       else if (c == '\b') { 
//         output+'\\'
//         output+'b'
//       }
//       else if (c == '\t') { 
//         output+'\\'
//         output+'t'
//       }
//       else if (c == '\r') { 
//         output+'\\'
//         output+'r'
//       }
//       else if (c == '\'') { 
//         output+'\\'
//         output+'\''
//       }
//       else if (c == '\"') { 
//         output+'\\'
//         output+'\''
//       }
//       else {  
//         output+c 
//       }
//     } 
//     return output.toString
//   } 

  // object Mode extends Enumeration {
  //      type Mode = Value
  //      val EscapeChar, Normal = Value
  // }

  // def UnEscapeString(s: String) : String = { 
  //      val replace_char = '\u001F'
  //      val output = new StringBuilder()
  //      import Mode._
  //      var status = Normal
  //      for (c: Char <- s) {
  //        if (c == '\\' && status == Normal) { 
  //           status = EscapeChar
  //        }
  //        else if (status == EscapeChar && c == replace_char) { 
  //           status = Normal
  //           output+','
  //        }
  //        else if (status == EscapeChar && c == 'n') { 
  //           status = Normal
  //           output+'\n'
  //        }
  //        else if (status == EscapeChar && c == 'r') { 
  //           status = Normal
  //           output+'\r'
  //        }
  //        else if (status == EscapeChar && c == 'b') { 
  //           status = Normal
  //           output+'\b'
  //        }
  //        else if (status == EscapeChar && c == 't') { 
  //           status = Normal
  //           output+'\t'
  //        }
  //        else if (status == EscapeChar && c == '\"') { 
  //           status = Normal
  //           output+'\"'
  //        }
  //        else if (status == EscapeChar && c == '\'') { 
  //           status = Normal
  //           output+'\''
  //        }
  //        else {  
  //           output+c 
  //           status = Normal
  //        }
  //      } 
  //      return output.toString
  // }

  //   /**
  //  * Test if the given byte array contains pickled data.
  //  */
  // def isPickled(bytes: Array[Byte]) = {
  //   val unpickle = new Unpickler
  //   var ret = true
  //   try {
  //     unpickle.load(bytes)
  //   } catch {
  //     _ => ret = false
  //   }
  //   return isp
  // }


    // // I assume batch encoding because the AutoBatchedPickler serializes 
    // // blocks at a time?
    // val toRDDArgs = makeArgs(outputDir = internalOutput, encoding = "batch", 
    //   schema = schemaString, rddType = "schemardd")
    // // pipe to Unity
    // val fnames = unityPipe(UnityMode.ToSFrame, toRDDArgs, javaRDDofPickles, envVars).collect()
    // // pipe to Unity
    // val fnames = unityPipe(UnityMode.ToSFrame, toRDDArgs, javaRDDofPickles, envVars).coallese(1)
    // // Pipe all the fnames through the worker
    // val concatSFrameArgs = makeArgs(outputDir = outputDir, encoding = prefix, 
    //   schema = schemaString, rddType = "schemardd")
    
    // new unityIterator(UnityMode.Concat, concatSFrameArgs, )



 //  def stringToByte(jRDD: JavaRDD[String]): JavaRDD[Array[Byte]] = { 
 //    jRDD.rdd.mapPartitions { iter =>
 //      iter.map { row =>
 //        row match {
 //            case str:String => new sun.misc.BASE64Decoder().decodeBuffer(str)
 //          //case str:String => UnEscapeString(str).toCharArray.map(_.toByte)
 //        } 
 //      }

 //    }
 //  }
 
   
 //  def byteToString(jRDD: JavaRDD[Array[Byte]]): JavaRDD[String] = { 
 //    jRDD.rdd.mapPartitions { iter =>
 //      iter.map { row =>
 //        row match {
 //          //case bytes:Array[Byte] => EscapeString(new String(bytes.map(_.toChar)))
 //           case bytes:Array[Byte] => new sun.misc.BASE64Encoder().encode(bytes).replaceAll("\n","")
 //        } 
 //      }

 //    }
 //  }
 
 //  def unEscapeJavaRDD(jRDD: JavaRDD[Array[Byte]]): JavaRDD[Array[Byte]] = {
 //     //val jRDD = pythonToJava(rdd)
 //     jRDD.rdd.mapPartitions { iter =>
 //      val unpickle = new Unpickler
 //      val pickle = new Pickler
 //      iter.map { row =>
 //        row match {
 //           //case obj: String => obj.split(",").map(UnEscapeString(_))
 //           case everythingElse: Array[Byte] => pickle.dumps(unpickle.loads(everythingElse) match {
 //             case str: String => UnEscapeString(str) 
 //           })
 //        }
 //      }
     
 //     }.toJavaRDD()
 //  } 
   
 // def toJavaStringOfValues(jRDD: RDD[Any]): JavaRDD[String] = {

 //  jRDD.mapPartitions { iter =>
 //      iter.map { row =>
 //        row match {
 //          case obj: Seq[Any] => obj.map { item => 
 //            item match {
 //              case str: String => EscapeString(str)
 //              case others: Any => others
 //            }
 //          }.mkString(",")
 //        }   
 //      }   
 //    }.toJavaRDD()
 //  }
 //  def splitUnEscapeJavaRDD(jRDD: JavaRDD[Array[Byte]]): JavaRDD[Array[String]] = {
 //     jRDD.rdd.mapPartitions { iter =>
 //      val unpickle = new Unpickler
 //      val pickle = new Pickler
 //      iter.map { row =>
 //        unpickle.loads(row) match {
 //           //case obj: String => obj.split(",").map(UnEscapeString(_))
 //           //case everything: Array[Byte] => pickle.dumps(unpickle.loads(everythingElse) match {
 //             case str: String => str.split(",").map(UnEscapeString(_))
 //           //}
 //        }
 //      }
 //     }.toJavaRDD()

 //  }

