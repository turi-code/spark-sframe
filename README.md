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
rdd = sc.parallelize(range(1, 100)).map(lambda x: (x, str(x)))
sframe = SFrame.from_rdd(rdd, sc)
print sframe
```

## Standalone Integration 
### Run Spark Shell
```bash
cd $SPARK_HOME
bin/spark-shell
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
