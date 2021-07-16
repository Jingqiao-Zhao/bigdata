package com.zhao.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

// 对两个数据源进行操作
object Spark_Transform_Double_Value {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    val listRDD2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6, 7, 8))

    val stringRDD1: RDD[String] = sc.makeRDD(List("Hadoop", "Hello", "Spark", "Scala"))
    val stringRDD2: RDD[String] = sc.makeRDD(List("Flink", "Hello", "Spark", "Scala"))

    val listListRDD: RDD[List[Int]] = sc.makeRDD(List(List(1, 2, 3), List(4, 5, 6)))

    // 交集，如果两个RDD数据类型不一样，编译不通过
    val value: RDD[Int] = listRDD1.intersection(listRDD2)

    // 并集，如果两个RDD数据类型不一样，编译不通过
    val value1: RDD[Int] = listRDD1.union(listRDD2)

    // 差集，如果两个RDD数据类型不一样，编译不通过
    val value2: RDD[Int] = listRDD1.subtract(listRDD2)

    // 拉链，对应位置放到一个集合，数据类型可以不一样
    // 如果分区数目不一样会出现错误，保持分区数目一致
    // 两个数据源的每一个分区元素数量要保证相同
    val value3: RDD[(Int, Int)] = listRDD1.zip(listRDD2)










  }
}
