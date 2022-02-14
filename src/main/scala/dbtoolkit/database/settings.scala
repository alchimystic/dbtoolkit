package dbtoolkit.database

import java.sql.{Connection, DriverManager}

import dbtoolkit.database.dialect.Dialect

case class DatabaseSettings(dialect: Dialect, url: String, username: String, password: String,
                            readOnly: Boolean = true, schema: Option[String]) {

  def getConnection: Connection = {
    Class.forName(dialect.driverClassname)
    val con = DriverManager.getConnection(url, username, password)
    con.setReadOnly(readOnly)
    con
  }

  def isLocal: Boolean = url.contains("localhost") || url.contains("127.0.0.1")
}
