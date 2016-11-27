package com.ibm.spark.test


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * Created by vinayak on 24/11/16.
 */
class SparkSQLWorkloadTask(name: String = "SQL Work") extends SparkWorkloadTask(name) {

  val spark = SparkSession
    .builder()
    .appName(name)
    .getOrCreate()

  override def execute(sparkContext: SparkContext): Unit = {
    val peopleJson = List(
      """{"name":"Michael"}""",
      """{"name":"Andy", "age":30}""",
      """{"name":"Justin", "age":19}""")

    val peopleRDD = sparkContext.makeRDD(peopleJson)

    val peopleDF = spark.read.json(peopleRDD)

    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by spark
    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()
  }
}
