package com.datio.sas

import java.io.File

import com.datio.sas.parquet.ParquetWriter
import com.datio.sas.csv.CsvReader
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

/**
  * Created by dvega on 19/05/17.
  */
object CsvParquet {
  def main(args: Array[String]): Unit = {

    val log = LoggerFactory.getLogger(this.getClass)

    val sparkConf = new SparkConf()

    val sc = new SparkContext(sparkConf)
    val sqc = new SQLContext(sc)

    val configFile = SparkFiles.get("application.conf")
    log.info(s"Path to application.conf is: $configFile")
    val catConfigFile = scala.io.Source.fromFile(configFile).mkString
    log.info(catConfigFile)

    val config = ConfigFactory.load(ConfigFactory.parseFile(new File(configFile)))

    val csvDF = new CsvReader(sqc)(config.getConfig("sas.csv")).read.collect()


    new ParquetWriter()(config.getConfig("sas.parquet")).write(csvDF)

    sc.stop()
  }
}
