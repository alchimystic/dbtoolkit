package dbtoolkit.database

import java.sql._

import dialect.Dialect

/**
  * Helper for reading DatabaseMetaData
  */
object MetadataReader {

  def getAllTableObjects(schema: Option[String])(implicit dialect: Dialect, md: DatabaseMetaData): List[ObjRef] = {
    getTables(schema, None)
      .map(tbl => ObjRef(tbl.schema, tbl.name))
  }

  def getTables(schema: Option[String], tablename: Option[String] = None)
               (implicit dialect: Dialect, md: DatabaseMetaData): List[Table] = {
    ResultSetReader.readAll(dialect.tableMetadataParser) {
      md.getTables(null, valueOf(schema), tablename.orNull, Seq("TABLE").toArray)
    } map { table =>
      val pks = ResultSetReader.readAll(dialect.pkMetadataParser) {
        md.getPrimaryKeys(valueOf(table.cat), valueOf(table.schema), table.name)
      }
      table.copy(primaryKeys = pks.toSet)
    }
  }

  def getColumns(table: ObjRef)
                (implicit dialect: Dialect, md: DatabaseMetaData): Map[String, Column] = {
    ResultSetReader.readAll(dialect.columnMetadataParser) {
      md.getColumns(valueOf(None), valueOf(table.schema), table.name, null)
    }.map { c =>
      c.name -> c
    }.toMap
  }

  def getColumns(table: Table)
                (implicit dialect: Dialect, md: DatabaseMetaData): List[Column] = {
    ResultSetReader.readAll(dialect.columnMetadataParser) {
      md.getColumns(valueOf(table.cat), valueOf(table.schema), table.name, null)
    } map { col =>
      col.copy(pk = table.primaryKeys(col.name))
    }
  }

  def getOrderedTables(links: List[KeyLink]): Seq[ObjRef] = {
    val allTables = links.map(e => List(e.fk.tableRef, e.pk.tableRef)).flatten.toSet
    val allDeps =
      links
      .map { e => (e.fk.tableRef, e.pk.tableRef) }
      .groupBy(_._1)
      .map { case (k,v) => k -> v.map(_._2).filterNot( _ == k) }
      .withDefault(_ => Nil)

    def _reduce(tables: Set[ObjRef], deps: Map[ObjRef, List[ObjRef]], acc: Seq[ObjRef]): Seq[ObjRef] = {
      tables.size match {
        case 0 => acc
        case _ =>
          val (indep, dep) = tables.partition(deps(_).isEmpty)
          val done = acc ++ indep.toSeq.sortBy(_.name) //sorting each table layer, so the order is the same from any machine
          val rest = deps.filterKeys(dep).mapValues(_.filterNot(done.toSet))
          _reduce(tables.filterNot(indep), rest, done)
      }
    }
    _reduce(allTables, allDeps, Nil)
  }

  private def valueOf[T](opt: Option[T]): T = opt.getOrElse(null.asInstanceOf[T])

}
