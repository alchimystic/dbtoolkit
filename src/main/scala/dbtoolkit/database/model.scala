package dbtoolkit.database

import java.sql.Types

import TextOps._

/**
  * Represents a table (related to DatabaseMetaData.getTables)
  * @param cat
  * @param schema
  * @param name
  * @param typ
  * @param remarks
  * @param primaryKeys
  */
case class Table(cat: Option[String],
                 schema: Option[String],
                 name: String,
                 typ: String,
                 remarks: String,
                 primaryKeys: Set[String] = Set.empty) {

  lazy val entityName = upper(underscoredToCamelCase(name))

  private lazy val prefix = s"${entityName.filter(_.isUpper).toLowerCase}_"

  def stripTablePrefix(colName: String): String = colName.startsWith(prefix) match {
    case false => colName
    case true => colName.substring(prefix.length)
  }
}

/**
  * Identifies an object inside a schema (table, view, type, etc..)
  * @param schema
  * @param name
  */
case class ObjRef(schema: Option[String], name: String) {
  lazy val ref: String = schema match {
    case None => name
    case Some(s) => s"$s.$name"
  }
}

/**
  * Represents a key
  * @param tableRef
  * @param columns
  */
case class Key(tableRef: ObjRef, columns: List[String])

/**
  * Represents a key row (part of a key, related to DatabaseMetaData.getXXXKeys)
  * @param tableRef
  * @param column
  * @param index
  */
case class KeyRow(tableRef: ObjRef, column: String, index: Int)

/**
  * Represents a key link (primary key to foreign key)
  * @param name
  * @param pk
  * @param fk
  */
case class KeyLink(name: String, pk: Key, fk: Key, follow: Boolean = true) {
  require(pk.columns.size == fk.columns.size)
  lazy val zippedColumns: List[(String, String)] = pk.columns.zip(fk.columns)
}

/**
  * Represents a table link
  * @param name
  * @param pkRef
  * @param pkColumn
  * @param fkRef
  * @param fkColumn
  * @param index
  */
case class TableLink(name: String, pkRef: ObjRef, pkColumn: String, fkRef: ObjRef, fkColumn: String, index: Int)

/**
  * Represents a Column (related to DatabaseMetaData.getColumns)
  * @param name
  * @param typeCode
  * @param typeName
  * @param nullable
  * @param pk
  */
case class Column(name: String,
                  typeCode: Int,
                  typeName: String,
                  nullable: Boolean,
                  pk: Boolean = false) {

  lazy val customTypeRef: Option[ObjRef] = typeCode match {
    case Types.VARCHAR => parseCustomTypeRef
    case Types.OTHER => parseCustomTypeRef //for custom type arrays
    case _ => None
  }

  lazy val customTypeArray: Boolean = customTypeRef.isDefined && typeName.contains(""""."_""")

  private def parseCustomTypeRef: Option[ObjRef] = typeName.contains('.') match {
    case false => None
    case true => typeName.filter(_ != '\"').split("\\.").toList match {
      case List(schema, name) =>
        val name2 = if (name.startsWith("_")) name.substring(1) else name
        Some(ObjRef(Some(schema), name2))
      case _ => throw new IllegalArgumentException(s"Parsing type ref from $typeName")
    }
  }

}

case class CompositeKey(parts: Seq[Column])

/**
  *  SQL data type abstraction
  */
sealed trait DataType

case class UnsupportedDataType(code: Int) extends DataType

/**
  * Simple type (mappable directly to a class)
  * @param clazz
  */
case class SimpleDataType(clazz: Class[_]) extends DataType

/**
  * Custom type (enum)
  * @param ref
  * @param enumType
  * @param array
  */
case class CustomDataType(ref: ObjRef, enumType: EnumDataType, array: Boolean = false) extends DataType {
  lazy val name: String = {
    TextOps.entityClassName(ref.name)
  }
}

case class EnumDataType(ref: ObjRef, name: String, values: List[String] = Nil)
