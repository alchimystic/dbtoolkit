package dbtoolkit

import java.sql.ResultSet
import java.time.ZonedDateTime

import dbtoolkit.database.dialect.Postgresql

package object database {

  type TypeResolver = ObjRef => EnumDataType

  /**
    * Parses T from a ResultSet
    * @tparam T
    */
  type RowParser[T] = ResultSet => T

  //maps (code, typename) to a class
  type TypeMapper = PartialFunction[(Int, String), Class[_]]

  import java.sql.Types._

  val defaultTypeMapper: TypeMapper = {
    case (VARCHAR, _) => classOf[String]
    case (INTEGER, _) => classOf[Int]
    case (SMALLINT, _) => classOf[Short]
    case (TINYINT, _) => classOf[Byte]
    case (BOOLEAN, _) => classOf[Boolean]
    case (BIT, _) => classOf[Boolean]
    case (TIMESTAMP, _) => classOf[ZonedDateTime]
    case (TIMESTAMP_WITH_TIMEZONE, _) => classOf[ZonedDateTime]
    case (ARRAY, "_text") => classOf[Array[String]]
    case (OTHER, "json") => classOf[String]
    //    case (OTHER, _) => classOf[String]
  }

  object Databases {
    val postgres = Postgresql
  }

}
