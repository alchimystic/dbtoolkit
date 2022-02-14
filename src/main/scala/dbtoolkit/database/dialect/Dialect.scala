package dbtoolkit.database.dialect

import java.sql.{Connection, DatabaseMetaData}
import dbtoolkit.database._

/**
  * Abstraction over a database vendor.
  * All database specific aspects should be added here.
  */
trait Dialect {

  def driverClassname: String

  /**
    * @return RowParser of Table
    */
  def tableMetadataParser: RowParser[Table]

  /**
    * @return RowParser of Column
    */
  def columnMetadataParser: RowParser[Column]

  /**
    * @return RowParser of ObjRef
    */
  def typeMetadataParser: RowParser[ObjRef]

  /**
    * @return RowParser of primary key
    */
  def pkMetadataParser: RowParser[String] = { rset => rset.getString(4) }

  /**
    * @return RowParser of TableLink
    */
  def importedTableLinkMetadataParser: RowParser[TableLink]

  /**
    * @return RowParser of KeyRow
    */
  def primaryKeyMetadataParser: RowParser[KeyRow]

  /**
    * @param md
    * @return all TYPE ObjRefs
    */
  def getTypeRefs()(implicit md: DatabaseMetaData): List[ObjRef] = {
    ResultSetReader.readAll(typeMetadataParser) {
      md.getTables(null,null,null,List("TYPE").toArray)
    }
  }

  def getEnumType(ref: ObjRef, namer: String => String)(implicit con: Connection): EnumDataType

  def getKeyLinks(schema: Option[String], table: Option[String])(implicit md: DatabaseMetaData): List[KeyLink] = {
    val rows = ResultSetReader.readAll(importedTableLinkMetadataParser) {
      md.getImportedKeys(null, schema.orNull, table.orNull)
    }

    rows.groupBy { e => e.name }.map {
      case ((name), list) =>
        val sorted = list.sortBy(_.index)
        val first = sorted.head
        KeyLink(name, Key(first.pkRef, sorted.map(_.pkColumn)), Key(first.fkRef, sorted.map(_.fkColumn)))
    }.toList
  }

  def getPrimaryKeys(schema: Option[String], table: Option[String])(implicit md: DatabaseMetaData): List[Key] = {
    val rows = ResultSetReader.readAll(primaryKeyMetadataParser) {
      md.getPrimaryKeys(null, schema.orNull, table.orNull)
    }

    rows.groupBy(_.tableRef).map { e =>
      Key(e._1, e._2.sortBy(_.column).map(_.column))
    }.toList
  }

  def getPrimaryKeyColumns(ref: ObjRef)(implicit md: DatabaseMetaData): List[Column] = {
    val tbl = MetadataReader.getTables(ref.schema, Some(ref.name))(this, md)(0)
    MetadataReader.getColumns(tbl)(this, md).filter{_.pk}
  }

  def selectQuery(objRef: ObjRef, values: List[(String, Any)]): String = {
    val where = values.map(_._1).map(c => s"$c = ?").mkString(" AND ")
    s"SELECT * FROM ${objRef.ref} WHERE $where"
  }

  def selectQuery(objRef: ObjRef, where: String): String = {
    s"SELECT * FROM ${objRef.ref} WHERE $where"
  }

  def selectQueryFull(objRef: ObjRef, cond: Option[String] = None): String = {
    val where = cond.map(s => s"WHERE $s").getOrElse("")
    s"SELECT * FROM ${objRef.ref} $where"
  }

  def insertQuery(objRef: ObjRef, data: List[(String, Any)], formatter: TableValueFormatter): String = {
    val data_ = data.filter(doInsert)
    val cols = data_.map(_._1).mkString(",")
    val values = data_.map( e => formatter.format(e._1, e._2)).mkString(",")
    s"INSERT INTO ${objRef.ref} ($cols) VALUES ($values)"
  }

  def doInsert(mapping: (String, Any)): Boolean = Option(mapping._2).filter(_ != "null").isDefined

  def deleteQuery(objRef: ObjRef, data: List[(String, Any)], formatter: TableValueFormatter): String = {
    val conditions = data.map {
      case (col, value) => s"$col = ${formatter.format(col, value)}"
    }.mkString(" AND ")
    s"DELETE FROM ${objRef.ref} WHERE $conditions"
  }

  def valueFormatter: ValueFormatter

  protected def quote(value: Any): String = s"""'$value'"""
}

trait ValueFormatter {
  def apply(typeCode: Int, value: Any): String
}

case class TableValueFormatter(formatter: ValueFormatter, cols: Map[String, Column]) {
  def format(colName: String, value: Any): String = {
    val col = cols(colName)
    Option(value) match {
      case None => null
      case _ => formatter.apply(col.typeCode, value)
    }
  }
}

object Dialect {
  def apply(name: String): Dialect = name match {
    case "Postgresql" => Postgresql
    case other => throw new UnsupportedOperationException(name)
  }
}
