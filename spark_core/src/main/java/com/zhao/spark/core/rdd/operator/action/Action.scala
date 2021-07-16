package com.zhao.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Action{

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))

    // TODO - 行动算子
    // 降维
    rdd.reduce(_+_)
    // 不同分区的数据采集到的Driver端内存中
    rdd.collect()
    // 计算RDD中的元素数目
    rdd.count()
    // 返回采集到的第一个元素
    rdd.first()
    // 获取N个数据
    rdd.take(3)
    // 获取排序后的N个数据
    rdd.takeOrdered(3)
    // 初始值，分区内，分区间的计算规则
    /* agregateByKey：只考虑分区内初始值额外计算
       agregate：分区间的初始值也考虑计算
    * */
    rdd.aggregate(0)(_+_,_+_)
    // 分区内和分区间的计算规则相同
    rdd.fold(0)(_+_)
    // 分区中每个值出现几次
    rdd.countByValue()
    // 如果直接使用不保证RDD的顺序，使用collect().foreach()保证分区的顺序
    /*collect().foreach()：Driver端内存数据打印
    * foreach()：在Executor端进行数据打印
    * */
    rdd.foreach(println)

    // RDD的方法区别于scala的方法集合的方法在Executor端进行执行，RDD方法外部操作在Driver端执行，内部的方法在Executor端执行

    // TODO save相关算子
    rdd.saveAsTextFile("txt")
    rdd.saveAsObjectFile("output1")
    // 必须是键值类型
    // rdd.saveAsSequenceFile("output3")


    sc.stop()

  }

}
