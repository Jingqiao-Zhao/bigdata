package com.zhao.spark.core.rdd.creat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {

    // 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(conf)

    // 创建RDD,从内存中创建RDD，将内存中的集合作为数据源。
    val seq: Seq[Int] = Seq[Int](1,2,3,4,5)
    // makeRDD可以确定分区，如果不传递默认使用默认并行度
    // 默认情况从配置对象中获取配置参数：Spark.default.parallelism，
    // 如果获取不到，使用totalCores属性当前环境的最大核数
    // 在分区的过程中
    val rdd: RDD[Int] = sc.makeRDD(seq,2)
    // 将分区的结果保存为文件
    rdd.saveAsTextFile("output")

    // 关闭环境
    sc.stop()
  }

}
