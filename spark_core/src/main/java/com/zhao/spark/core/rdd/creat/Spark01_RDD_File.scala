package com.zhao.spark.core.rdd.creat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_File {
  def main(args: Array[String]): Unit = {

    // 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(conf)

    // 创建RDD,从内存中创建RDD，将文件中作为数据源。
    // Path的路径以当前的根路径为主，可绝对路径可相对路径
    // Path可以是文件具体目录也可以是目录名称
    // 也可以使用通配符*
    val rdd: RDD[String] = sc.textFile("datas")
    // 读取文件的同时读取路径
    val rdd2: RDD[(String, String)] = sc.wholeTextFiles("datas")


    rdd.collect().foreach(println)
    rdd2.collect().foreach(println)
    // 关闭环境
    sc.stop()
  }

}
