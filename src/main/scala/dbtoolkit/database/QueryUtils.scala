package dbtoolkit.database

import java.sql.{Connection, ResultSet, ResultSetMetaData}

import dbtoolkit.database.QueryUtils.getMultiple

/**
  * Helper for executing SQL queries and returning their results
  */
object QueryUtils {

  /**
    * Gets all rows from the given query/args
    * @param sql query prepared statement like (with ? placeholders)
    * @param values values to set in the statement
    * @param con connection
    * @return seq of Map[column name -> column value]
    */
  def getAll(sql: String, values: Seq[(String, Any)])(implicit con: Connection): Seq[Map[String, Any]] = {
    getMultiple(sql, values, Integer.MAX_VALUE)
  }

  /**
    * Gets the first rows from the given query/args
    *
    * @param sql query prepared statement like (with ? placeholders)
    * @param values values to set in the statement
    * @param con connection
    * @return optional Map[column name -> column value]
    */
  def getFirst(sql: String, values: Seq[(String, Any)])(implicit con: Connection): Option[Map[String, Any]] = {
    getMultiple(sql, values, 1).headOption
  }

  private def read(rs: ResultSet, md: ResultSetMetaData): Map[String, Any] = {
    val values = for {
      i <- 0 until md.getColumnCount
      col = md.getColumnName(i + 1)
      value = rs.getObject(i + 1)
    } yield (col -> value)
    values.toMap
  }

  private def getMultiple(sql: String, values: Seq[(String, Any)], limit: Int)(implicit con: Connection): Seq[Map[String, Any]] = {
    val prep = con.prepareStatement(sql)
    val wCounter = new Counter

    try {
      values.foreach { e =>
        prep.setObject(wCounter.next, e._2)
      }

      println(s"executing $sql")

      val rs = prep.executeQuery()
      val md = rs.getMetaData
      var result: Seq[Map[String, Any]] = Nil

      try {
        while (rs.next() && result.size < limit) {
          result = result :+ read(rs, md)
        }
        result
      } finally {
        rs.close()
      }
    } finally {
      prep.close()
    }
  }

  private class Counter {
    var idx = 0

    def next: Int = {
      idx = idx + 1
      idx
    }
  }

}
