package dbtoolkit

import java.io.File

import dbtoolkit.common.ToolConfig

object Main {

  def main(args: Array[String]): Unit = {
    val reader = ToolConfig(new File(args(0)))
    val dialect = reader.dialect
    val schema = reader.schema
    val dbSettings = reader.dbSettings("localIntra")
    println(dbSettings)
  }

}
