package com.ibm.spark.test
import scala.io.StdIn

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by vinayak on 27/11/16.
 */
class SparkStorageWorkloadTask (name: String = "Storage Work") extends SparkWorkloadTask(name){

  override def execute(sc: SparkContext): Unit = {

    val data = "Apache Spark provides programmers with an application programming interface " +
      "centered on a data structure called the resilient distributed dataset (RDD), a read-only " +
      "multiset of data items distributed over a cluster of machines, that is maintained in a " +
      "fault-tolerant way.[2] It was developed in response to limitations in the MapReduce " +
      "cluster computing paradigm, which forces a particular linear dataflow structure on " +
      "distributed programs: MapReduce programs read input data from disk, map a function across " +
      "the data, reduce the results of the map, and store reduction results on disk. Spark's RDDs" +
      " function as a working set for distributed programs that offers a (deliberately) " +
      "restricted form of distributed shared memory."

    val wordsRDD = sc.parallelize(data.split(" "))

    wordsRDD.persist()

    val wordLengths = wordsRDD.map (word => word.length)

    val totalWordLength = wordLengths.reduce((a, b) => a+b)

    val numwords = wordsRDD.count()

    println(s"total wl: $totalWordLength, word count:$numwords")
  }
}
