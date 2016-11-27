package com.ibm.spark.test.sparkworkload.test

import com.ibm.spark.test.SparkWorkload
import org.scalatest.{FunSuite, Matchers}

/**
 * Created by vinayak on 27/11/16.
 */
class SparkWorkloadSuite extends FunSuite with Matchers {

  test("test no args") {

    var conf = SparkWorkload.parseArgs(Array(""))

    assert(conf.iterations == 0)
    assert(conf.waitAfterComplete == false)
    assert(conf.duration == 0)
  }

  test("test args 1") {

    var conf = SparkWorkload.parseArgs("--iterations 3".split(" "))

    assert(conf.iterations == 3)
    assert(conf.waitAfterComplete == false)
    assert(conf.duration == 0)
  }

  test("test args 2") {

    var conf = SparkWorkload.parseArgs("--iterations 3 --duration 9".split(" "))

    assert(conf.iterations == 3)
    assert(conf.waitAfterComplete == false)
    assert(conf.duration == 9)
  }

  test("test args 3") {

    var conf = SparkWorkload.parseArgs("--iterations 3 --wait".split(" "))

    assert(conf.iterations == 3)
    assert(conf.waitAfterComplete == true)
    assert(conf.duration == 0)
  }

  test("test args 4") {

    var conf = SparkWorkload.parseArgs("--iterations 3 --duration 4 --wait".split(" "))

    assert(conf.iterations == 3)
    assert(conf.waitAfterComplete == true)
    assert(conf.duration == 4)
  }

  test("test args 5") {

    var conf = SparkWorkload.parseArgs("--duration 3".split(" "))

    assert(conf.iterations == 0)
    assert(conf.waitAfterComplete == false)
    assert(conf.duration == 3)
  }

  test("test args 6") {

    var conf = SparkWorkload.parseArgs("--duration 6 --wait".split(" "))

    assert(conf.iterations == 0)
    assert(conf.waitAfterComplete == true)
    assert(conf.duration == 6)
  }

  test("test args 7") {

    var conf = SparkWorkload.parseArgs("--wait".split(" "))

    assert(conf.iterations == 0)
    assert(conf.waitAfterComplete == true)
    assert(conf.duration == 0)
  }
}
