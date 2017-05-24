package com.datio.sas.csv

import com.datio.sas.model.Schemas
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

import scala.collection.JavaConverters._

/**
  * Class to parse a CSV using spark-csv library and its options.
  * Takes an implicit parameter which is a Config following this format.
  * {
  * path = "/tmp/file.csv"
  * options {
  * compression = "snappy"
  *     deflate.level = "5"
  * ...
  * }
  * }
  * To see othe options check out https://github.com/databricks/spark-avro or reference.conf file
  */
class CsvReader(sqc: SQLContext)(implicit config: Config) {

  /**
    * Read a Dataframe using spark csv library.
    *
    * @return a Dataframe containing the csv info.
    */
  def read: DataFrame = {

    val path = config.getString("path")
    val dataReader = sqc.read.format("com.databricks.spark.csv")
    val schema = config.getString("schema")
    setOptions(dataReader).schema(Schemas.getSchema(schema)).load(path)

  }

  /**
    * Apply options from configuration to the DataFrame reader.
    *
    * @param dfr DataFrameReader you want to assign options.
    * @return a DataFrameReader with options assigned.
    */
  protected def setOptions(dfr: DataFrameReader): DataFrameReader = {
    val optionsConfig = config.getConfig("options")
    val entries = optionsConfig.entrySet().asScala.toList

    entries.map(_.getKey).foldLeft(dfr)((dfr, key) => dfr.option(key, optionsConfig.getString(key)))
  }
}
