package dbtoolkit.database

import java.sql.ResultSet

/**
  * Helper for reading ResultSets
  * @param rs
  */
class ResultSetReader(rs: ResultSet) {

  private var currentIdx = 1

  def setIndex(value: Int): ResultSetReader = {
    currentIdx = value
    this
  }

  private def get[T](f: Int => T): T = {
    val idx = currentIdx
    currentIdx = currentIdx + 1
    f(idx)
  }

  private def getOpt[T](f: Int => T): Option[T]  = {
    val value = get(f)
    rs.wasNull() match {
      case true => None
      case false => Some(value)
    }
  }

  def readAsBool: Boolean = readString == "YES"

  def readString: String = get(rs.getString)

  def readStringOpt: Option[String] = getOpt(rs.getString)

  def readInt: Int = get(rs.getInt)

  def readIntOpt: Option[Int] = getOpt(rs.getInt)

}

object ResultSetReader {

  def readAll[T](parser: RowParser[T])(rset: ResultSet): List[T] = {

    def _read(): List[T] = {
      rset.next() match {
        case true => parser(rset) :: _read()
        case false => Nil
      }
    }
    val result = _read()
    rset.close()
    result
  }


  def inspect(rset: ResultSet): Unit = {
    val rmd = rset.getMetaData
    val count = rmd.getColumnCount
    val cols = for (i <- 1 to count) yield (i, rmd.getColumnLabel(i))
    cols.foreach(println)
  }

  def explore(rs: ResultSet): Unit = {
    val rsmd = rs.getMetaData
    val keys = for (i <- 1 to rsmd.getColumnCount) yield rsmd.getColumnName(i)

    while (rs.next()) {
      println(keys.zipWithIndex.map { e => e._1 -> rs.getObject(e._2 + 1) }.toMap)
    }

    rs.close()
  }
}
