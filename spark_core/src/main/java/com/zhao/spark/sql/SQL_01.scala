package com.zhao.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SQL_01 {

  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL运行环境

    val sparConf: SparkConf = new SparkConf().setMaster("local").setAppName("SQL")
    val spark: SparkSession = SparkSession.builder().config(sparConf).getOrCreate()
    // TODO 执行逻辑

    // DataFrame
    val df: DataFrame = spark.read.json("datas/user.json")
    //SQL方法
    df.createOrReplaceTempView("user")
    spark.sql("select * from user").show()
    // UDF函数
    spark.udf.register("prefixName",(name:String)=>("name" + name))
    spark.sql("select prefixName(name) from user").show()
    //DSL方法,隐式转换
    import spark.implicits._
    df.select("username").show()
    df.select($"age" + 1).show()
    // df.show()



    // DataSet

    // RDD <=> DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "张三", 30), (2, "李四", 25)))
    val df2: DataFrame = rdd.toDF("id", "name", "age")
    val rdd1: RDD[Row] = df2.rdd

    // DataFrame <=> DataSet

    case class User(id:Int,name:String,age:Int)
    val ds: Dataset[User] = df2.as[User]
    val frame: DataFrame = ds.toDF()
     // RDD <=> DataSet
     val value: Dataset[User] = rdd.map {
       case (id, name, age) => User(id, name, age)
     }.toDS()
    // TODO 关闭连接
    spark.stop()
  }

}
