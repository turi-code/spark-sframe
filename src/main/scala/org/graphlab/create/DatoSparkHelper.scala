package org.apache.spark.dato

import java.io.File

import org.apache.spark.api.python._

/**
 * This object contains helper functions that can reach into
 * Spark's private (and therefore constantly evolving) APIs
 * and borrow their configuration
 */
object DatoSparkHelper {

  def sparkPythonPath = {
    PythonUtils.sparkPythonPath
  }

}
