package com.zhao.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Serial {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val search: Search = new Search("a")

    search.getMatch2(rdd).collect().foreach(println)

    sc.stop()
  }
  //查询对象
  class Search(query:String) extends Serializable {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {

      rdd.filter(isMatch)
    }
    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {

      rdd.filter(x => x.contains(query))

    }
  }
}
