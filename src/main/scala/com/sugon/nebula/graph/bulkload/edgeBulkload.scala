package com.sugon.nebula.graph.bulkload

import com.sugon.nebula.graph.common.sparkFuncs
import com.vesoft.nebula.connector.connector.{NebulaDataFrameReader, NebulaDataFrameWriter}
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig, WriteMode, WriteNebulaEdgeConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object edgeBulkload {


  def main(args: Array[String]): Unit = {
    val spark = sparkFuncs.init_local_spark("nebua_test")
//    readEdge(spark)
    storeEdge(spark)
  }


  def readEdge(spark:SparkSession): Unit ={

    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("172.22.5.12:9559")
      .withConenctionRetry(2)
      .withExecuteRetry(2)
      .withTimeout(6000)
      .build()

    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("graphspace_test")
      .withLabel("follow")
      .withNoColumn(false)
      .withReturnCols(List("start_zjhm","end_zjhm","start_person","end_person","start_time","end_time"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val edge = spark.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToDF()
    edge.show(truncate = false)
  }


  def storeEdge(spark:SparkSession): Unit ={
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("172.22.5.12:9559")
      .withGraphAddress("172.22.5.12:9669")
      .withConenctionRetry(2)
      .build()

    val schema = StructType(
      StructField("start_jid", StringType)::
        StructField("end_jid", StringType)::
        StructField("start_zjhm", StringType)::
        StructField("end_zjhm", StringType)::
        StructField("start_person", StringType)::
        StructField("end_person", StringType)::
        StructField("start_time", LongType)::
        StructField("end_time", LongType)::
        Nil
    )

    val  followDF = spark.read.schema(schema).format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header","true")
      .option("sep","\t")
      .load("D:\\sg_idea_workspace\\graph-spark3\\data\\nebula_follow.csv")

    followDF.show(truncate = false)
    followDF.printSchema()

    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace("graphspace_test")
      .withEdge("follow")
      .withSrcIdField("start_jid")
      .withDstIdField("end_jid")
      .withUser("root")
      .withWriteMode(WriteMode.UPDATE)
      .withPasswd("nebula")
      .withBatch(1000)
      .build()
    followDF.write.nebula(config, nebulaWriteEdgeConfig).writeEdges()
  }

}
