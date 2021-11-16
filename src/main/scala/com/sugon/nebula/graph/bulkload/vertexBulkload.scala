package com.sugon.nebula.graph.bulkload

import com.sugon.nebula.graph.common.sparkFuncs
import com.vesoft.nebula.connector.connector.{NebulaDataFrameReader, NebulaDataFrameWriter}
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig, WriteMode, WriteNebulaVertexConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object vertexBulkload {


  def main(args: Array[String]): Unit = {
    val spark = sparkFuncs.init_local_spark("nebua_test")
//    readVertex(spark)
    storeVertex(spark)
  }


  def readVertex(spark:SparkSession): Unit ={

    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("172.22.5.12:9559")
      .withConenctionRetry(2)
      .withExecuteRetry(2)
      .withTimeout(6000)
      .build()

    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("graphspace_test")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List("zjhm","xm","age"))
      .withLimit(10)
      .withPartitionNum(2)
      .build()
    val vertex = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
    vertex.printSchema()
    vertex.show(truncate = false)

  }


  def storeVertex(spark:SparkSession): Unit ={
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("172.22.5.12:9559")
      .withGraphAddress("172.22.5.12:9669")
      .withConenctionRetry(2)
      .build()

    val schema = StructType(
      StructField("jid", LongType)::
        StructField("zjhm", StringType)::
        StructField("xm", StringType)::
        StructField("age", IntegerType)::
        Nil
    )

    val  personDF = spark.read.schema(schema).format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header","true")
      .option("sep","\t")
      .load("D:\\sg_idea_workspace\\graph-spark3\\data\\person.csv")
      .drop("jid")
      .selectExpr("MD5(concat_ws('_',zjhm,xm,age)) jid","zjhm","xm","age")

    personDF.show(truncate = false)
    personDF.printSchema()

    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("graphspace_test")
      .withTag("person")
      .withVidField("jid")
      .withUser("root")
      .withPasswd("nebula")
      .withWriteMode(WriteMode.INSERT)
      .withBatch(1000)
      .build()

    personDF.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  }

}
