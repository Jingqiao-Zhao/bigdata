package com.zhao.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_Transform_Key_Value {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)

    val mapRDD: RDD[(Int, Int)] = listRDD1.map((_, 1))

    val kvRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 2), ("a", 3), ("b", 8)),2)
    val kvRDD1: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("a", 3), ("b", 5), ("a", 2), ("b", 1)),2)
    // 根据指定的分区规则进行重分区
    // 如果两个分区器相同返回自身
    // RangePartitioner和排序有关的分区器
    // 可以自定义分区器
    val unit: Unit = mapRDD.partitionBy(new HashPartitioner(2))


    // 相同的key进行Value的聚合操作，spark基于scala开发，两两聚合
    // 如果key只有一个不进行两两运算
    val reduce: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => (x + y))

    // 相同key的数据分在同一组中，元组中第一个元素为key，第二个元素为相同key的value的集合
    // 和groupby的区别是不会把key单独拿出来
    // groupby会进行落盘操作，spark中的shuffle操作必须落盘处理，shuffle的操作性能非常低
    // reducebykey在落盘之前会在之前再分区中进行预聚合，减少数据量，速度快一些
    val groupRDD: RDD[(String, Iterable[Int])] = kvRDD.groupByKey()

    // 业务需求，分区内相同的取最大值，分区外求和
    // 存在函数科里化，两个参数列表
    // 第一个参数列表为初始值
    //    主要用于碰到第一个key的时候和第一个值计算
    //    返回值的数据类型与初始值的数据类型相同
    // 第二个参数列表传递两个参数
    //   第一个为分区内的计算规则
    //   第二个为分区间的计算规则
    val value: RDD[(String, Int)] = kvRDD.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )
    // 要求取得相同key的数据平均值 => (a,2)(b,5)
    val value2: RDD[(String, Int)] = kvRDD.aggregateByKey((0, 0))(
      (t, v) => (t._1 + v, t._2 + 1),
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)
    ).mapValues { case (num, cnt) => num / cnt }

    //当分区内和分区外的计算相同时使用
    val value1: RDD[(String, Int)] = kvRDD.foldByKey(0)(_ + _)

    // combineByKey：方法需要三个参数
    // 第一个参数：将相同的key的第一个数据进行结构的转换
    // 第二个参数：分区内的计算规则
    // 第三个参数：分区外的计算规则
    val value3: RDD[(String, (Int, Int))] = kvRDD.combineByKey(
      // 第一个值不进行运算只转换结构
      // a,1 => a (1,1)
      v => (v, 1),
      (t: (Int, Int), v) => (t._1 + v, t._2 + 1),
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    )

    /*
    * reduceByKey、 foldByKey、 aggregateByKey、 combineByKey
    * 底层都是
    * combineByKeyWithClassTag(createCombiner,  第一个数据处理方法
    *                          mergeValue,      分区内的操作方法
    *                          mergeCombiners,  分区外的操作方法
    *                                      )
    * */

    // 当拥有不同的key的时候，不同的key不会返回
    // 当存在多个重复的key时挨个匹配，返回会出现多个结果
    // 若存在大量相同的key数据量会几何性增长
    val value4: RDD[(String, (Int, Int))] = kvRDD.join(kvRDD1)

    // 左链接和右连接
    kvRDD.leftOuterJoin(kvRDD1)
    kvRDD.rightOuterJoin(kvRDD1)


    sc.stop()

  }
}
