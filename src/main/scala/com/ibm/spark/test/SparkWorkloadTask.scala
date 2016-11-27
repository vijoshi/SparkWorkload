package com.ibm.spark.test

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by vinayak on 24/11/16.
 */
abstract class SparkWorkloadTask(name: String) {

  def execute(sparkContext: SparkContext): Unit

}
