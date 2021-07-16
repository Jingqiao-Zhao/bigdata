package com.zhao.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


//对单个数据源进行操作
object Spark_Transform_Single_Value {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    val stringRDD: RDD[String] = sc.makeRDD(List("Hadoop", "Hello", "Spark", "Scala"))
    val listListRDD: RDD[List[Int]] = sc.makeRDD(List(List(1, 2, 3), List(4, 5, 6)))
    // 待处理的数据逐条映射或者类型转换
    listRDD.map(_*2)

    // 以分区为单位进行计算节点的处理，相对于map速度快但是内存消耗大
    listRDD.mapPartitions(
      data => data
    )

    // 将处理的数据进行扁平化的映射
    listListRDD.flatMap(
      list => list
    )

    // 将一个分区内的数据直接转换为相同类型的内存数组进行处理，分区不变
    listRDD.glom()

    // 根据指定规则进行分组，分区默认不变，但是数据会打乱重新组合，最坏的情况数据会在一个分区
    // 一个组的数据在一个分区，但并不是一个分区只有一个组
    // 根据奇偶数分组
    listRDD.groupBy(_%2)
    // 根据首字母分组
    stringRDD.groupBy(_.charAt(0))

    // 根据指定的规则进行过滤，符合的保留，不符合的丢弃，
    // 数据过滤后分区不变但是分区内的数据可能不均衡，生产环境可能出现数据倾斜
    // 保留开头为H的字符串
    stringRDD.filter(_.charAt(0)=='H')

    // 指定规则数据集中抽取数据，抽奖用？分为泊松分布和伯努利分布，不常用
    listRDD.sample(true,0.5,1234)

    // 去重，实现原理reduceByKey
    listRDD.distinct()

    // 缩减分区，数据过滤后提高小数据集的执行效率，减少任务调度成本
    listRDD.coalesce(2)

    // 分区数目转换，可以多转少或者少转多
    listRDD.repartition(4)

    // 排序算子，不改变分区结果，中间存在shuffle操作，默认升序，加false为降序
    listRDD.sortBy(num=>num)




  }

}
