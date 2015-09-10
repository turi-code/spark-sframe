package org.graphlab.create

import java.io._
import java.nio.charset.Charset
import net.razorvine.pickle.{Pickler, Unpickler}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkContext, _}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.dato.DatoSparkHelper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.EvaluatePython

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source


// REQUIRES JAVA 7.  
// Consider switching to http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/IOUtils.html#toByteArray%28java.io.InputStream%29
import java.nio.file.{Files, Paths}

object GraphLabUtil {


  /**
   * Utility function needed to get access to GraphLabUtil object
   * from GraphLab python interface.
   */
  def getUtil() = this


  /**
   * The types of unity mode supported and their corresponding
   * command line arguments
   */
  object UnityMode extends Enumeration {
    type UnityMode = Value
    val ToSFrame = Value(" --mode=tosframe ")
    val ToRDD = Value(" --mode=tordd ")
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
    if (!success) {
      println("Error making " + outputDir.toString)
    }
    // @todo: Do something about when not success.
    outputDir.toString
  }


  /**
   * Merge paths with the appropriate separator. Ignores blank strings.
   *
   * Borrowed directly from PythonUtils.scala in pyspark
   */
  def mergePaths(paths: String*): String = {
    paths.filter(_ != "").mkString(File.pathSeparator)
  }


  /**
   * Write an integer in native order.
   */
  def writeInt(out: java.io.OutputStream, x: Int) {
    out.write(x.toByte)
    out.write((x >> 8).toByte)
    out.write((x >> 16).toByte)
    out.write((x >> 24).toByte)
  }


  /**
   * Read an integer in native ordering.
   */
  def readInt(in: java.io.InputStream): Int =  {
    in.read() | (in.read() << 8) | (in.read() << 16) | (in.read() << 24)
  }


  /**
   * Expand the current path into individual file paths.
   */
  def expandPath(inputPath: String, recursive: Boolean = false): Array[String] = {
    val paths = inputPath.split(java.io.File.pathSeparator)
    paths.filter(s => !s.isEmpty).flatMap { pathStr =>
      val f = new java.io.File(pathStr)
      if (f.isDirectory) {
        val sep =
          if (pathStr.last.toString != java.io.File.separator) {
            java.io.File.separator
          } else { "" }
        if (recursive) {
          f.list.flatMap { x =>
            val newPath = pathStr + sep + x
            expandPath(newPath, recursive)
          }
        } else {
          f.list.map { x => pathStr + sep + x }
        }
      } else {
        List(pathStr)
      }
    }
  }


  /**
   * Install the bundled binary in this jar stored in:
   *     src/main/deps/org/graphlab/create 
   * in the temporary directory location used by Spark.
   */
  def installBinary(binName: String, as: String) {
    val rootDirectory = SparkFiles.getRootDirectory()
    val outputPath = Paths.get(rootDirectory, as)
    this.synchronized {
      if (!outputPath.toFile.exists) {
        println(s"Installing binary $binName as $as")
        println(s"\t to ${outputPath.toString}")
        // Get the binary resources bundled in the jar file
        // Note that the binary must be located in:
        //   src/main/resources/org/graphlab/create/
        val in = GraphLabUtil.getClass.getResourceAsStream(binName)
        Files.copy(in, outputPath)
        outputPath.toFile.setExecutable(true)
      }
    }
  }


  /**
   * Install the bundled binary with the same 
   * name as it is stored in the jar.
   */
  def installBinary(name: String) {
    installBinary(name, name)
  }


  /**
   * Get the current hadoop version
   */
  def getHadoopVersion: String = {
    val conf = new org.apache.hadoop.conf.Configuration
    conf.get("hadoop.common.configuration.version")
  }


  /**
   * Get the name for libhdfs for this platform
   */
  def getLibHDFSName: String = {
    val platform = getPlatform
    if (platform == "mac") {
      "libhdfs.dylib"
    } else if (platform == "linux") {
      "libhdfs.so"
    } else { // windows
      "hdfs.dll"
    }
  }


  /**
   * Get the name for libjvm on this platform
   */
  def getLibJVMName: String = {
    val platform = getPlatform
    if (platform == "mac") {
      "libjvm.dylib"
    } else if (platform == "linux") {
      "libjvm.so"
    } else { // windows
      "jvm.dll"
    }
  }


  /**
   * Install the correct platform specific libhdfs binary
   */
  def installLibHDFS() {
    val libHDFSName = getLibHDFSName
    installBinary(libHDFSName)
  }


  /**
   * Get the platform specific lib search path
   */
  def getLibSearchPath: String = {
    val sys = System.getProperties
    mergePaths(sys.getOrElse("sun.boot.library.path", ""),
      sys.getOrElse("sun.boot.library.path", ""),
      sys.getOrElse("java.library.path", ""),
      sys.getOrElse("sun.boot.library.path", "") + File.separator + "server")
    //   sys.getOrElse("spark.executor.extraLibraryPath", "")
  }


  /**
   * Get the local path for the given library name 
   */ 
  def getLibPath(libName: String): String = {
    expandPath(getLibSearchPath, recursive=false)
      .filter(_.contains(libName)).mkString(File.pathSeparator)
  }


  /**
   * Get the library path for the spark_unity binary
   */
  def getUnityLibPath: String = {
    // currently we are installing our own libhdfs.so so we need to
    // just find the libjvm path. In later releases we should consider
    // resolving the path for libhdfs locally.
    getLibSearchPath
  }


  /**
   * Get the classpath being used by spark.  
   */
  def getClasspath: String = {
    val sys = System.getProperties
    val classPath = sys.getOrElse("java.class.path", "")
    classPath
    // Originally I had assumed we needed the expanded classpath (as
    // described by the JNI docs) however this does not appear to be
    // the case perhaps if there are class path issues consider adding
    // the additional code below

    // + java.io.File.pathSeparator +
    //  expandPath(classPath, recursive=true).mkString(java.io.File.pathSeparator)
  }


  /**
   * Get the platform information as a simple string: mac, linux, windows
   */
  def getPlatform: String = {
    val osName = System.getProperty("os.name")
    if (osName == "Mac OS X") {
      "mac"
    } else if (osName.toLowerCase.contains("windows")) {
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
  def getBinaryName: String = {
    "spark_unity_" + getPlatform
  }


  /**
   * Install all platform specific binaries in the temporary 
   * working directory.
   */
  def installPlatformBinaries() {
    installBinary(getBinaryName)
  }


  /**
   * Get the default namenode for this hadoop cluster.
   */
  def getHadoopNameNode: String = {
    val conf = new org.apache.hadoop.conf.Configuration
    conf.get("fs.default.name")
  }


  /**
   * Get the default namenode for this spark context
   */
  def getHadoopNameNode(sc: SparkContext): String = {
    val conf = sc.hadoopConfiguration
    conf.get("fs.default.name")
  }


  /**
   * Get the environment link variable for this platform.
   * @return
   */
  def getPlatformLinkVar = {
    val platform = getPlatform
    if (platform == "mac") {
      "DYLD_FALLBACK_LIBRARY_PATH" // This is safer than DYLD_LIBRARY_PATH
    } else if (platform == "linux") {
      "LD_LIBRARY_PATH"
    } else { // windows
      "PATH"
    }
  }


  /**
   * Build and launch a process with the appropriate arguments.
   *
   */
  def launchProcess(mode: UnityMode, args: String): Process = {
    // Install the binaries in the correct location:
    installPlatformBinaries()
    // Much of this code is "borrowed" from org.apache.spark.rdd.PippedRDD
    // Construct the process builder with the full argument list 
    // including the unity binary name and unity mode
    // @todo it is odd that I needed the ./ before the filename to execute it with process builder
    val launchName = "." + java.io.File.separator + getBinaryName.trim()
    val fullArgList = (List(launchName, mode.toString) ++ args.split(" ")).map(_.trim).filter(_.nonEmpty)
    // Display the command being run
    println("Launching Unity: \n\t" + fullArgList.mkString(" "))
    val pb = new java.lang.ProcessBuilder(fullArgList)
    // Add the environmental variables to the process.
    val env = pb.environment
    // val thisEnv = System.getenv
    // thisEnv.foreach { case (variable, value) => env.put(variable, value) }
    // Getting the current python path and adding a separator if necessary

    val pythonPath = mergePaths(env.getOrElse("PYTHONPATH", ""),
      DatoSparkHelper.sparkPythonPath)
    env.put("PYTHONPATH", pythonPath)

    // set the ld_library_path
    env.put(getPlatformLinkVar, getUnityLibPath)

    try {
      installLibHDFS()
    } catch {
      case e: Exception =>
        System.err.println("Error installing native libhdfs relying on environment.")
    }

    val classpath = getClasspath
    println("Classpath: " + classpath)
    env.put("CLASSPATH", classpath)

    // Set the working directory 
    pb.directory(new File(SparkFiles.getRootDirectory()))
    // Luanch the graphlab create process
    val proc = pb.start()
    // Start a thread to print the process's stderr to ours
    new Thread("GraphLab Unity Standard Error Reader") {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines()) {
          System.err.println("UNITY MSG: \t" + line)
        }
      }
    }.start()
    proc
  }


  /**
   * Write a message in the binary format of:
   *
   *    signed 32bit int (message Length in bytes)
   *    message bytes
   *
   *
   */
  def writeMessage(out: OutputStream, bytes: Array[Byte]): Unit = {
    writeInt(out, bytes.length)
    out.write(bytes)
  }


  /**
   * Write the message encoding end of file (msglen = -1)
   */
  def writeEndOfFileMessage(out: OutputStream): Unit = {
    writeInt(out, -1)
  }


  /**
   * This function takes an iterator over arrays of bytes and executes
   * the unity binary
   */
  private def toSFrameIterator(args: String, iter: Iterator[Array[Byte]]): Iterator[String] = {
    // Launch the unity process
    val proc = launchProcess(UnityMode.ToSFrame, args)
    // Start a thread to feed the process input from our parent's iterator
    new Thread("GraphLab Unity toSFrame writer") {
      override def run() {
        val out = proc.getOutputStream
        iter.foreach(bytes => writeMessage(out, bytes))
        writeEndOfFileMessage(out)
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
   * This function creates an iterator that returns arrays of pickled bytes read from
   * the unity process.
   *
   * @param partId the partition id of this iterator
   * @param numPart the number of partitions
   * @param args additional arguments constructed for the unity process
   * @return
   */
  private def toRDDIterator(partId: Int, numPart: Int, args: String): Iterator[Array[Byte]] = {
    // Update the Args list with the extra information
    val finalArgs = args + s" --numPartitions=$numPart --partId=$partId "
    // Launch the unity process
    val proc = launchProcess(UnityMode.ToRDD, finalArgs)
    // Create an iterator that reads bytes directly from the unity process
    new Iterator[Array[Byte]] {
      // Binary input stream from the process standard out
      val in = proc.getInputStream
      // The number of bytes to read next (may be 0 or more)
      var nextBytes = readInt(in)
      // Retunr the next array of bytes which could have length 0 or more.
      def next(): Array[Byte] = {
        // Verify that we have bytes to read
        if (nextBytes < 0) {
          throw new Exception("Reading past end of SFrame")
        }
        // Allocate a buffer and read the bytes
        val buffer = new Array[Byte](nextBytes)
        val bytesRead = in.read(buffer)
        // Verify that we read enough bytes
        if (bytesRead != nextBytes) {
          throw new Exception("Error in reading SFrame")
        }
        // Get the length of the next frame of bytes
        nextBytes = readInt(in)
        // Return the buffer
        buffer
      }
      // There are additional bytes to read if nextBytes >= 0
      def hasNext: Boolean = nextBytes >= 0
    }
  }


  /**
   * This function takes a collection of sframes and concatenates
   * them into a single sframe
   *
   */
  private def concat(sframes: Array[String], args: String): String = {
    // Launch the graphlab unity process
    val proc = launchProcess(UnityMode.Concat, args)

    // Write all the filenames to standard in for the child process
    val out = new PrintWriter(proc.getOutputStream)
    sframes.foreach(out.println)
    out.close()

    // wait for the child process to terminate
    val exitStatus = proc.waitFor()
    if (exitStatus != 0) {
      throw new Exception("Subprocess exited with status " + exitStatus)
    }

    // Get an iterator over the output
    val outputIter = Source.fromInputStream(proc.getInputStream).getLines()
    if (!outputIter.hasNext) {
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
   * @param outputDir The directory in which to store the SFrame
   * @param prefix the filename prefix to use for the final SFRame
   * @param additionalArgs any additional arguments that can be provided by python (e.g., data encoding)
   * @return the final filename of the output sframe.
   */
  def pySparkToSFrame(jrdd: JavaRDD[Array[Byte]], outputDir: String, prefix: String, additionalArgs: String): String = {
    // Create folders
    val internalOutput: String =
      makeDir(new org.apache.hadoop.fs.Path(outputDir, "internal"), jrdd.sparkContext)
    // println("Made dir: " + internalOutput)
    val argsTooSFrame = additionalArgs +
      s" --outputDir=$internalOutput " +
      s" --prefix=$prefix"
    // pipe to Unity 
    val fnames = jrdd.rdd.mapPartitions (
      (iter: Iterator[Array[Byte]]) => { toSFrameIterator(argsTooSFrame, iter) }
    ).collect()
    val argsConcat = additionalArgs +
      s" --outputDir=$outputDir " +
      s" --prefix=$prefix"
    val sframe_name = concat(fnames, argsConcat)
    sframe_name
  }


  /**
   * This debug operation is used to save the binary pickles passed into the spark_unity binary via standard in.
   *
   * @param df input dataframe to pickle
   * @param prefix prefix of each file in which to save the debug binary pickles
   */
  def saveDebugPickles(df: DataFrame, prefix: String) {
    val blocks = pickleDataFrame(df).rdd.mapPartitions(iter => Iterator(iter.toArray)).collect()
    for (i <- blocks.indices) {
      val f = new java.io.FileOutputStream(prefix + "_" + i.toString)
      blocks(i).foreach(m => writeMessage(f, m))
      writeEndOfFileMessage(f)
      f.close()
    }
  }


  /**
   * Pickle a Spark DataFrame using the spark internals
   *
   */
  def pickleDataFrame(df: DataFrame): JavaRDD[Array[Byte]] = {
    val fieldTypes = df.schema.fields.map(_.dataType)
    // The first block of bytes in the dataframe will be in the format
    //
    // int: numCols
    // loop {
    //   int:   colName.length
    //   bytes: colName (utf8)
    //   int:   colType.length
    //   bytes: colType text (utf8)
    // }
    val bos = new ByteArrayOutputStream()
    writeInt(bos, fieldTypes.length)
    val utf8 = Charset.forName("UTF-8")
    for (f <- df.schema.fields) {
      val nameBytes = f.name.getBytes(utf8)
      // val typeBytes = f.dataType.typeName.getBytes(utf8)
      val typeBytes = f.dataType.simpleString.getBytes(utf8)
      writeMessage(bos, nameBytes)
      writeMessage(bos, typeBytes)
    }
    bos.flush()
    // save the byte signature
    val bytes = bos.toByteArray

    // Convert the data frame into an RDD of byte arrays
    df.rdd.mapPartitions { rowIterator =>
      val arrayIterator = rowIterator.map(
        row => row.toSeq.zip(fieldTypes).map {
          case (field, fieldType) =>
            val value = EvaluatePython.toJava(field, fieldType)
            if (value.isInstanceOf[collection.mutable.WrappedArray[_]]) {
              // This is a strange bug but Spark is wrapping its arrays in the java
              // conversion which is breaking the razorvine serialization.
              // by converting the wrapper array back to a standard array seralization works
              value.asInstanceOf[collection.mutable.WrappedArray[_]].toArray
            } else {
              value
            }
        }.toArray
      )
      Iterator(bytes) ++ new AutoBatchedPickler(arrayIterator)
    }.toJavaRDD()
  }


  /**
   * Take a dataframe and convert it to an sframe
   */
  def toSFrame(df: DataFrame, outputDir: String, prefix: String): String = {
    // Convert the dataframe into a Java rdd of pickles of batches of Rows
    val javaRDDofPickles = pickleDataFrame(df)
    // Construct the arguments to the graphlab unity process
    val args = " --encoding=batch --type=dataframe "
    pySparkToSFrame(javaRDDofPickles, outputDir, prefix, args)
  }


  /**
   * This is a special implementation for processing RDDs of strings
   *
   * @param rdd the rdd of strings to save as an SFrame
   * @param outputDir the output directory in which to save the SFrames
   * @param prefix the file prefix to uses for the final SFrame files
   *
   * @return the filename of the final SFRame
   */
  def toSFrame(rdd: RDD[String], outputDir: String, prefix: String): String = {
    val javaRDDofUTF8Strings: RDD[Array[Byte]] = rdd.mapPartitions { iter =>
      val cs = Charset.forName("UTF-8")
      iter.map(s => s.getBytes(cs))
    }
    // Construct the arguments to the graphlab unity process
    val args = " --encoding=utf8 --type=rdd "
    pySparkToSFrame(javaRDDofUTF8Strings, outputDir, prefix, args)
  }


  /**
   * Load an SFrame into a JavaRDD of Pickled objects.
   */
  def pySparkToRDD(sc: SparkContext, sframePath: String, numPartitions: Int, additionalArgs: String): JavaRDD[Array[Byte]] =  {
    // @todo we currently use the --outputDir option to encode the input dir (consider changing)
    val args = additionalArgs + s"--outputDir=$sframePath"
    val pickledRDD = sc.parallelize(0 until numPartitions, numPartitions).mapPartitionsWithIndex {
      (partId: Int, iter: Iterator[Int]) => toRDDIterator(partId, numPartitions, args)
    }
    pickledRDD.toJavaRDD()
  }


  /**
   * Load an SFrame into an RDD of dictionaries
   *
   * @todo convert objects into Dataframe rows
   *
   * @param sc the spark context to use
   * @param sframePath the path to the sframe index file
   * @param numPartitions the number of partitions for the final RDD
   */
  def toRDD(sc: SparkContext, sframePath: String, numPartitions: Int): RDD[java.util.HashMap[String, _]] = {
    val args = "" // currently no special arguments required?
    // Construct an RDD of pickled objects
    val pickledRDD = pySparkToRDD(sc, sframePath, numPartitions, args).rdd
    // Unpickle the
    pickledRDD.mapPartitions { iter =>
      val unpickle = new Unpickler
      iter.map(unpickle.loads(_).asInstanceOf[java.util.HashMap[String, _]])
    }
  }


  /**
   * Load an SFrame into an RDD of dictionaries
   *
   * @param sc the spark context to use
   * @param sframePath the path to the sframe index file
   */
  def toRDD(sc: SparkContext, sframePath: String): RDD[java.util.HashMap[String, _]] = {
    toRDD(sc, sframePath, sc.defaultParallelism)
  }

} // End of GraphLabUtil
