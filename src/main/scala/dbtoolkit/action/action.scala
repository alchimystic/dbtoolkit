package dbtoolkit.action

import java.sql.Connection

import dbtoolkit.database.DataTool.ValueSet
import dbtoolkit.database.{ObjRef, ResultSetReader}

case class TargetEntity(table: ObjRef, ids: List[ValueSet])

sealed trait EntityResolver {
  def apply(): TargetEntity
}

sealed trait DataSelector {
  def getIds(con: Connection): List[Any]
  val tablename: String
}

case class IdList(tablename: String, ids: List[Any]) extends DataSelector {
  override def getIds(con: Connection): List[Any] = ids
}

case class IdQuery(tablename: String, query: String) extends DataSelector {
  override def getIds(con: Connection): List[Any] = {
    val prep = con.prepareStatement(query)
    val rset = prep.executeQuery()
    try {
      ResultSetReader.readAll({ rs => rs.getObject(1) })(rset)
    } finally {
      rset.close()
      prep.close()
    }
  }
}
