# Spark Unity Codebase
This project contains the code to interact with the GraphLab Create open-source project from within Apache Spark.  Currently, the jar created by this project is included in the GraphLab Create python egg to enable translation between Apache Spark Dataframes and GraphLab Create SFrames.  Users can also use this project in the scala spark shell to export Dataframes as SFrames.

## PySpark Integration 
### Run PySpark
```bash
cd $SPARK_HOME
bin/pyspark
```
### Make an SFrame from an RDD
```python
from graphlab import SFrame
rdd = sc.parallelize([(x,str(x), "hello") for x in range(0,5)])
sframe = SFrame.from_rdd(rdd, sc)
print sframe
```
### Make an SFrame from a Dataframe (preferred)
```python
from graphlab import SFrame
rdd = sc.parallelize([(x,str(x), "hello") for x in range(0,5)])
df = sql.createDataFrame(rdd)
sframe = SFrame.from_rdd(df, sc)
print sframe
```

## Standalone Integration 
### Run Spark Shell
```bash
cd $SPARK_HOME
bin/spark-shell --jars platform-spark/target/GraphLabSpark-0.1-SNAPSHOT.jar
```
### Make an SFrame from an RDD
```scala
import org.graphlab.create.GraphLabUtil
df = sc.parallelize(range(1, 100)).toDF // Must be a dataframe
val outputDir = "/tmp/graphlab_testing" // Must be an HDFS path unless running in local mode
val prefix = "test"
val sframeFileName = GraphLabUtil.toSFrame(df, outputDir, prefix)
print sframeFileName
```
### Make an RDD from an SFrame
```scala
import org.graphlab.create.GraphLabUtil
val newRDD = GraphLabUtil.toRDD(sc, "/tmp/graphlab_testing/test.frame_idx")
```

# Requirements and Caveats
The currently release requires Python 2.7, Spark 1.3 or later, and the `hadoop` binary must be within the `PATH` of the driver when running on a cluster or interacting with `Hadoop` (e.g., you should be able to run `hadoop classpath`).

We also currently only support Mac and Linux platforms but will have Windows support soon. 
