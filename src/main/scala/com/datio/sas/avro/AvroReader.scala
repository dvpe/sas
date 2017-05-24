package com.datio.sas.avro

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

import scala.collection.JavaConverters._

/**
  * Class to write in Avro using spark-avro library and its options.
  * Takes an implicit parameter which is a Config following this format.
  * {
  * path = "/path/to/target/"
  * options {
  *   compression = "deflate"
  *   deflate.level = "5"
  *   ...
  * }
  * }
  * To see othe options check out https://github.com/databricks/spark-avro
  */
class AvroReader(sqc: SQLContext)(implicit config: Config) {

  /**
    * Read a Dataframe using spark csv library.
    *
    * @return a Dataframe containing the csv info.
    */
  def read: DataFrame = {
    val path = config.getString("path")
    val dataReader = sqc.read
    setOptions(dataReader).parquet(path)
  }

  /**
    * Apply options from configuration to the DataFrame writer.
    *
    * @param dfw
    * @return
    */
  protected def setOptions(dfr: DataFrameReader): DataFrameReader = {
    val optionsConfig = config.getConfig("options")
    val entries = optionsConfig.entrySet().asScala.toList

    entries.map(_.getKey).foldLeft(dfr)((dfr, key) => dfr.option(key, optionsConfig.getString(key)))
  }
}
