package com.datio.sas

import java.io.File

import com.datio.sas.avro.AvroWriter
import com.datio.sas.csv.CsvReader
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.slf4j.LoggerFactory

object CsvAvro {

  def main(args: Array[String]): Unit = {

    val log = LoggerFactory.getLogger(this.getClass)

    val sparkConf = new SparkConf()

    val ss = SparkSession.builder().config(sparkConf).master("local").getOrCreate()
    val sqlc = ss.sqlContext

    val configFile = SparkFiles.get("application.conf")
    log.info(s"Path to application.conf is: $configFile")
    val catConfigFile = scala.io.Source.fromFile(configFile).mkString
    log.info(catConfigFile)

    val config = ConfigFactory.load(ConfigFactory.parseFile(new File(configFile)))

    val csvDF = new CsvReader(sqlc)(config.getConfig("sas.csv")).read
    new AvroWriter(ss)(config.getConfig("sas.avro")).write(csvDF)

    ss.stop()
  }

}
