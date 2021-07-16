package com.zhao.spark.core.rdd.creat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {

    // 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(conf)

    // 创建RDD,从内存中创建RDD，将内存中的集合作为数据源。
    val seq: Seq[Int] = Seq[Int](1,2,3,4,5)
    // val rdd: RDD[Int] = sc.parallelize(seq), 两者除了方法的名字没有区别
    val rdd: RDD[Int] = sc.makeRDD(seq)

    rdd.collect().foreach(println)
    // 关闭环境
    sc.stop()
  }

}
