package com.sugon.nebula.graph.common



import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.Map

// 自定义函数或通用函数模块
class sparkFuncs{}

object sparkFuncs {
  val logger: Logger = Logger.getLogger(classOf[sparkFuncs])

  def init_local_spark(appName:String,options:Map[String,String] = Map()): SparkSession ={

    val sparkConf = new SparkConf()
    sparkConf.set("spark.executor.cores","1")
    sparkConf.set("spark.executor.instances","1")
    sparkConf.set("spark.shuffle.partitions","1")
    sparkConf.set("spark.default.parallelism","1")
    sparkConf.setMaster("local[8]")
    val spark = SparkSession.builder()
      .config(conf = sparkConf)
      .appName(appName)
      .getOrCreate()
    spark
  }

  def main(args: Array[String]): Unit = {

    val spark = init_local_spark("test")

  }

}
