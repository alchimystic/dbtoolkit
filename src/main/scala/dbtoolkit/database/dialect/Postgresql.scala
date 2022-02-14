package dbtoolkit.database.dialect

import java.sql.Connection

import dbtoolkit.database._

/**
  * Dialect impl for postgresql
  */
case object Postgresql extends Dialect {
  override def driverClassname: String = "org.postgresql.Driver"

  override val tableMetadataParser: RowParser[Table] = { rset =>
    val r = new ResultSetReader(rset)
    Table(r.readStringOpt, r.readStringOpt, r.readString, r.readString, r.readString)
  }

  override val typeMetadataParser: RowParser[ObjRef] = { rset =>
    val r = new ResultSetReader(rset)
    ObjRef(r.setIndex(2).readStringOpt, r.readString)
  }

  override val columnMetadataParser: RowParser[Column] = { rset =>
    val r = new ResultSetReader(rset).setIndex(4)
    Column(r.readString, r.readInt, r.readString, r.setIndex(18).readAsBool)
  }

  override def importedTableLinkMetadataParser: RowParser[TableLink] = { rset =>
    val fkRef = ObjRef(Option(rset.getString(6)), rset.getString(7))
    val pkRef = ObjRef(Option(rset.getString(2)), rset.getString(3))
    TableLink(rset.getString(12), pkRef, rset.getString(4), fkRef,  rset.getString(8), rset.getInt(9))
  }

  override def primaryKeyMetadataParser: RowParser[KeyRow] = { rset =>
      KeyRow(ObjRef(Option(rset.getString(2)),
        rset.getString(3)), rset.getString(4), rset.getInt(5))
  }

  override def getEnumType(ref: ObjRef, namer: String => String)(implicit con: Connection): EnumDataType = {
    val target = ref.schema match {
      case None => ref.name
      case Some(s) => s"$s.${ref.name}"
    }
    val prep = con.prepareStatement(s"SELECT unnest(enum_range(NULL::$target))::text")
    val rset = prep.executeQuery()
    try {
      val values = ResultSetReader.readAll { rs => rs.getString(1) } (rset)
      EnumDataType(ref, namer(ref.name), values)
    } finally {
      rset.close()
      prep.close()
    }
  }

  override val valueFormatter: ValueFormatter = new ValueFormatter {
    override def apply(typeCode: Int, value: Any): String = typeCode match {
      case 12 => quote(normalize(value.toString)) //text
      case 93 => quote(value) //timestamptz
      case 91 => quote(value) //java.sql.Date
      case 2003 => quote(value.toString) //_text???
      case 1111 => quote(value.toString) //json / array
      case 4 =>  value.toString //serial
      case -7 =>  value.toString //bit
      case -5 =>  value.toString //long
      case 8 =>  value.toString //double
      case other => throw new UnsupportedOperationException(s"Value formatter for type $other (Value is of type ${value.getClass.getName})")
    }
  }

  def normalize(input: String): String = {
    val normalized = input.contains(System.lineSeparator()) match {
      case true => input.replace(System.lineSeparator().charAt(0), ' ')
      case false => input
    }

    normalized.replace('\'', ' ')
  }
}
