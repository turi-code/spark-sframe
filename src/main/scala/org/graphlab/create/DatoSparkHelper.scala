/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
package org.apache.spark.dato

import java.io.File

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * This object contains helper functions that can reach into
 * Spark's private (and therefore constantly evolving) APIs
 * and borrow their functionality.
 *
 * This code will need to evolve with Spark.
 */
object DatoSparkHelper {


  /**
   * This function pulls the python path out of Spark by reach into an interal private method
   * to construct the python path.
   *
   * @return the python path for this platform.
   */
  def sparkPythonPath = {
    PythonUtils.sparkPythonPath
  }


}
