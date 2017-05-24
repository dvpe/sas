package com.datio.sas.csv

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, DataFrameWriter, SQLContext}
import java.io.File

/**
  * Created by davidvegaperez on 24/5/17.
  */
class CsvWriterProbando(sqc: SQLContext)(implicit config: Config) {
  /**
    * Write a dataframe in CSV.
    *
    * @param df Dataframe you want to write.
    */
  def write(df: DataFrame): Unit = {
    val path = config.getString("path")
    val data = df.rdd.map(row => row.toSeq.mkString(",")).collect()


    printToFile(new File(path)) { p =>
      data.foreach(p.println)
    }
    /*val dataWriter = df.coalesce(1).write.format("com.databricks.spark.csv")
    val savemode = Option(config.getString("savemode"))
      .getOrElse("ErrorIfExists").toLowerCase() match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "errorifexists" | _ => SaveMode.ErrorIfExists
    }
    setOptions(dataWriter).mode(savemode).save(path)*/
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  /**
    * Apply options from configuration to the DataFrame writer.
    *
    * @param dfw
    * @return
    */
  /*protected def setOptions(dfw: DataFrameWriter): DataFrameWriter = {
    val optionsConfig = config.getConfig("options")
    val entries = optionsConfig.entrySet().asScala.toList

    entries.map(_.getKey).foldLeft(dfw)((dfr, key) => dfr.option(key, optionsConfig.getString(key)))
  }*/
}
