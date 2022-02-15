package dbtoolkit.database

import java.sql.{Connection, DatabaseMetaData}
import com.typesafe.config.Config
import dbtoolkit.common.Collector
import dbtoolkit.database.DataTool.{IdResolver, ValueSet}
import dbtoolkit.database.ValueTransformers.ValueSetTransformer
import dbtoolkit.database.dialect.{Dialect, TableValueFormatter}

import scala.collection.JavaConverters._
import scala.collection.MapView

//TODO
//-add formatter support
object DataTool {

  type ValueSet = Map[String, Any]
  type ValueSorter = ((ValueSet, ValueSet), (ValueSet, ValueSet)) => Boolean
  type IdResolver = Any => ValueSet

}

object ValueParser {
  def apply(tp: String, value: String): Any = tp match {
    case "Int" => value.toInt
    case "Long" => value.toLong
    case other => throw new UnsupportedOperationException(s"Value type $other")
  }
}

/**
  * Definition of the target data (tables) to retrieve.
  */
class DataSelection(db: DatabaseSettings,
                    customLinks: List[KeyLink],
                    linkExcludes: Set[String],
                    val skipTables: Set[ObjRef],
                    val transformation: DataTransformation,
                    val rowFilters: List[RowFilter],
                    val initialTable: Option[ObjRef] = Option.empty,
                    val fixedTables: Set[ObjRef] = Set.empty,
                    val tableFilters: TableFilters = TableFilters(Map.empty),
                    val customKeys: List[Key] = Nil
                   )
                   (implicit dialect: Dialect, con: Connection, dmd: DatabaseMetaData) {

  lazy val pks = {
    val derived = dialect.getPrimaryKeys(db.schema, None).map { pk => pk.tableRef -> pk.columns }.toMap
    val custom = customKeys
      .map(k => k.tableRef -> k.columns)
      .filter(p => !derived.contains(p._1))
      .toMap

    derived ++ custom
  }

  lazy val links = dialect.getKeyLinks(db.schema, None) ++ customLinks

  lazy val selfReferencing = links.filter { k => k.pk.tableRef == k.fk.tableRef }.map(_.pk.tableRef).toSet

  lazy val orderedTables = fixedTables.toList ++ MetadataReader.getOrderedTables(links)

  lazy val formatters: Map[ObjRef, TableValueFormatter] = orderedTables.map {
    ref => ref -> TableValueFormatter(dialect.valueFormatter, MetadataReader.getColumns(ref))
  }.toMap

  lazy val tablesOfInterest: Set[ObjRef] = {
    def _findLinked(tables: Set[ObjRef], linkss: List[KeyLink], acc: Set[ObjRef]): Set[ObjRef] = {
      val (deps, other) = linkss.partition { l => acc(l.pk.tableRef) && tables(l.fk.tableRef) }
      deps.filterNot { l => acc(l.fk.tableRef) }.map(_.fk.tableRef) match {
        case Nil => acc
        case list =>
          val set = list.toSet
          _findLinked(tables.filterNot(set), other, acc ++ set)
      }
    }

    _findLinked(orderedTables.toSet, links, initialTable.toSet)
  }

  lazy val validationError: Option[String] = {
    if (links.isEmpty) Some("No links, no tables")
    else None
  }

  /**
    * @param link key link
    * @return true if we want to retrieve the all the dependent rows in the given key link
    */
  def followDependents(link: KeyLink) = {
    link.follow && !linkExcludes.contains(link.name) && !skipTables(link.fk.tableRef)
  }

  def includeRow(table: ObjRef, row: ValueSet, checkTableSkip: Boolean = true): Boolean = {
    checkTableSkip && skipTables(table) match {
      case true => false //ignoring table
      case false => rowFilters.filter(_.table == table) match {
        case Nil => true //no filters
        case list =>
          list.forall(_.accept(row)) //all filters must pass
      }
    }
  }

  def createIdResolver(table: ObjRef): IdResolver = {
    val keys = dialect.getPrimaryKeyColumns(table)
    keys.size match {
      case 1 =>
        val col = keys(0).name

        return { id => Map(col -> id) }
      case 0 => throw new IllegalArgumentException("Table has no primary key")
      case _ => throw new UnsupportedOperationException("Table has more than one primary key column")
    }
  }

}

case class DataTransformation(transformers: Map[ObjRef, ValueSetTransformer], fallback: ValueSetTransformer) {
  def getTransformer(ref: ObjRef): ValueSetTransformer = transformers.get(ref).getOrElse(fallback)
}

case class TableFilters(conditions: Map[ObjRef, String])

class DataTool(selection: DataSelection)
              (implicit dialect: Dialect, con: Connection, dmd: DatabaseMetaData) {

  private var rows: Map[ObjRef, Map[ValueSet, ValueSet]] = Map.empty

  private var alreadyAddedCache: Set[Task[_]] = Set.empty

//  def fetchReferenceTables(): Unit = {
//    config.referenceTables.foreach(fetchAll(_))
//  }

  def fetchAllTables(): Unit = {
    selection.orderedTables.foreach(fetchAll(_))
  }

  def fetchAll(ref: ObjRef): Unit = processAll(Seq(RetrieveFull(ref)))

  def fetchAll(ref: ObjRef, filter: ValueSet): Unit = processAll(Seq(RetrieveAll(ref, filter)))

  def fetchAll(ref: ObjRef, query: String): Unit = processAll(Seq(RetrieveAllByQuery(ref, query)))

  def fetch(ref: ObjRef, id: ValueSet): Unit = processAll(Seq(RetrieveOne(ref, id)))

  private def processAll(tasks: Seq[Task[_]]): Unit = tasks match {
    case Nil =>
    case list =>
      val first = list.head
      val followUps = process(first)
      val next = (list.tail ++ followUps).toSet.filterNot(alreadyAddedCache).toList
      processAll(next)
  }

  def onInsertStatements(tables: List[ObjRef], collector: Collector[String]): Unit = {

    tables.flatMap { ref =>
      println(s"handling inserts for $ref")
      rows.get(ref) match {
        case None => Nil
        case Some(map) =>
          selection.pks
          //val keys = selection.pks(ref).toSet
          val formatter = selection.formatters(ref)
          val rowsForInsert = getRowsForInsert(ref, map)
          if (rowsForInsert.size == 0) {
            throw new IllegalStateException(s"No rows to insert for $ref")
          }
          rowsForInsert.map { el =>
            //collector(dialect.insertQuery(ref, transformer(el).toList, formatter))
            collector(dialect.insertQuery(ref, el.toList, formatter))
          }
      }
    }
  }

  def onInsertStatements(collector: Collector[String]): Unit = onInsertStatements(selection.orderedTables, collector)

  def onDeleteStatements(includeShared: Boolean, collector: Collector[String]): Unit = {
    val allTables = selection.orderedTables.reverse
    val tables = includeShared match {
      case true => allTables
      case false => allTables.filter(selection.tablesOfInterest)
    }
    //println(s"delete order: ${tables.map(_.name)}")

    tables.flatMap { ref =>
      rows.get(ref) match {
        case None => Nil
        case Some(map) =>
          val keys = selection.pks(ref).toSet
          val formatter = selection.formatters(ref)
          val byPkOnly = map.view.filterKeys(_.keySet == keys) //keeping only rows by pk

          //          val byPkOnly = map.filterKeys(_.keySet == keys) //keeping only rows by pk
          getRowsForDelete(ref, byPkOnly).map { el =>
            println(el)
            collector(dialect.deleteQuery(ref, el.toList, formatter))
          }
      }
    }
  }

  private def sorter(o1: (ValueSet, ValueSet), o2: (ValueSet, ValueSet)): Boolean = {
    o1._1.head._2.toString.toInt < o2._1.head._2.toString.toInt
  }

  private def reverseSorter(o1: (ValueSet, ValueSet), o2: (ValueSet, ValueSet)): Boolean = sorter(o2, o1)

  private def getRowsForInsert(ref: ObjRef, map: Map[ValueSet, ValueSet]): List[ValueSet] = map.isEmpty match {
    case true => Nil
    case false => selection.selfReferencing(ref) match {
      case true =>
        val keys = selection.pks(ref)
        if (keys.size == 1 && map.head._1.head._2.isInstanceOf[Comparable[_]] ) {
          map.toList.sortWith(sorter).map(_._2)
        } else {
          ??? //TODO to proper sort by analyzing the object dependencies
        }
      case false =>
        val unique = map.values.toSet //eliminating duplicates
        unique.toList.sortBy(_.hashCode()) //set some order (to keep result consistent among runs over same data)
    }
  }

  private def getRowsForDelete(ref: ObjRef, map: MapView[ValueSet, ValueSet]): List[ValueSet] = map.isEmpty match {
    case true => Nil
    case false => selection.selfReferencing(ref) match {
      case true =>
        val keys = selection.pks(ref)
        if (keys.size == 1 && map.head._1.head._2.isInstanceOf[Comparable[_]] ) {
          map.toList.sortWith(reverseSorter).map(_._1)
        } else {
          ??? //TODO to proper sort by analyzing the object dependencies
        }
      case false =>
        map.keys.toList.sortBy(_.hashCode()) //set some order (to keep result consistent among runs over same data)
    }
  }

  def stats(): Unit = {
    println("  STATS")
    selection.orderedTables.foreach { ref =>
      rows.get(ref) match {
        case None =>
        case Some(list) => println(s"${ref.name}: ${list.size}")
      }
    }
  }

  private def add(objRef: ObjRef, ids: ValueSet, values: ValueSet): Unit = {
    //println(s"Adding $ids")
    rows.get(objRef) match {
      case None => rows = rows.updated(objRef, Map(ids -> values))
      case Some(map) => rows = rows.updated(objRef, map.updated(ids, values))
    }
  }

  private def get(objRef: ObjRef, ids: ValueSet): Option[ValueSet] = rows.flatMap(_._2.get(ids)).headOption

  private def onlyKeys(objRef: ObjRef, values: ValueSet): ValueSet = {
    val pks = selection.pks
    val keys = selection.pks(objRef)
    values.filter { e => keys.contains(e._1) }
  }

  private def process(task: Task[_]): Seq[Task[_]] = {
    println(s"processing $task")
    alreadyAddedCache = alreadyAddedCache + task
    val result = task match {
      case r: RetrieveOne =>
        get(r.ref, r.id) match {
          case None =>
            r.execute(con) match {
              case Some(values) =>
                add(r.ref, r.id, getTransformedValues(r.ref, values))
                getDeps(r.ref, values)
              case None => Nil
            }
          case Some(_) => Nil //we assume its already checked
        }
      case r: RetrieveAll =>
        r.execute(con).map { row =>
          val transformed = getTransformedValues(r.ref, row)
          val ids = onlyKeys(r.ref, transformed)
          add(r.ref, ids, transformed)
          getDeps(r.ref, row)
        }.flatten
      case r: RetrieveFull =>
        r.execute(con).map { row =>
          val transformed = getTransformedValues(r.ref, row)
          val ids = onlyKeys(r.ref, transformed)
          add(r.ref, ids, transformed)
          Nil
        }.flatten

      case r: RetrieveAllByQuery =>
        r.execute(con).map { row =>
          val transformed = getTransformedValues(r.ref, row)
          val ids = onlyKeys(r.ref, transformed)
          add(r.ref, ids, transformed)
          Nil
        }.flatten

    }
//    result.foreach { v => println(s"  Adding $v") }
    result
  }



  private def getTransformedValues(ref: ObjRef, values: ValueSet): ValueSet = {
    val result = selection.transformation.getTransformer(ref)(values)
    result
  }

  private def validKey(cols: Seq[(String, String)], values: ValueSet): Option[ValueSet] = {
    val key: ValueSet = cols.map {
      case (pk, fk) => pk -> values(fk)
    }.toMap

    key.forall { e =>  Option(e._2).isDefined } match {
      case true => Some(key)
      case false => None
    }
  }

  private def getDeps(objRef: ObjRef, values: ValueSet): Seq[Task[_]] = {
    val dependencies = for {
      l <- getKeys(objRef, true)
      key <- validKey(l.zippedColumns, values)
    } yield (RetrieveOne(l.pk.tableRef, key))

    val dependents = for {
      l <- getKeys(objRef, false) if selection.followDependents(l)
      fk = l.zippedColumns.map { e => e._2 -> values(e._1) }.toMap
    } yield (RetrieveAll(l.fk.tableRef, fk))

    (dependencies ++ dependents).filterNot(alreadyAddedCache)
  }

  def taskCount: Int = alreadyAddedCache.size

  def debug[T](x: T, msg: => String): Option[T] = {
    println(msg)
    Option(x)
  }

  private def getKeys(ref: ObjRef, fk: Boolean): List[KeyLink] = {
    fk match {
      case true => selection.links.filter(_.fk.tableRef == ref)
      case false => selection.links.filter(_.pk.tableRef == ref)
    }
  }

  sealed trait Task[T] {
    def ref: ObjRef
    def values: ValueSet
    def execute(con: Connection): T
  }

  case class RetrieveOne(ref: ObjRef, id: ValueSet) extends Task[Option[ValueSet]] {

    id.foreach {
      case (k, v) => require(Option(v).isDefined, s"Invalid value for $ref")
    }

    override def execute(con: Connection): Option[ValueSet] = {
      val idList = id.toList
      val query = dialect.selectQuery(ref, idList)
      QueryUtils.getFirst(query, idList)(con).filter(selection.includeRow(ref, _))
    }

    override def values: ValueSet = id
  }

  case class RetrieveFull(ref: ObjRef) extends Task[Seq[ValueSet]] {
    override def execute(con: Connection): Seq[ValueSet] = {
      val filter = selection.tableFilters.conditions.get(ref)
      val query = dialect.selectQueryFull(ref, filter)
      val all = QueryUtils.getAll(query, Seq.empty)(con)
      all.filter(selection.includeRow(ref, _, false))
    }
    override def values: ValueSet = Map.empty
  }

  case class RetrieveAll(ref: ObjRef, values: ValueSet) extends Task[Seq[ValueSet]] {
    override def execute(con: Connection): Seq[ValueSet] = {
      val list = values.toList
      val query = dialect.selectQuery(ref, list)
      QueryUtils.getAll(query, list)(con).filter(selection.includeRow(ref, _))
    }
  }

  case class RetrieveAllByQuery(ref: ObjRef, where: String) extends Task[Seq[ValueSet]] {
    override def values: ValueSet = Map.empty
    override def execute(con: Connection): Seq[ValueSet] = {
      val query = dialect.selectQuery(ref, where)
      QueryUtils.getAll(query, Nil)(con).filter(selection.includeRow(ref, _))
    }
  }
}

trait RowFilter {
  def table: ObjRef
  def accept(row: ValueSet): Boolean
}

object RowFilter {
  def apply(cfg: Config, schema: Option[String]): RowFilter = {
    cfg.getString("kind") match {
      case "ColumnRowFilterIn" =>
        val table = cfg.getString("table")
        val col = cfg.getString("col")
        val valueType = cfg.getString("valueType")
        val values = cfg.getStringList("values").asScala.toList.map(ValueParser(valueType, _)).toSet
        ColumnRowFilterIn(ObjRef(schema, table), col, values)
      case "ColumnRowFilterBiggerThan" =>
        val table = cfg.getString("table")
        val col = cfg.getString("col")
        val valueType = cfg.getString("valueType")
        val value = ValueParser(valueType, cfg.getString("value"))
        ColumnRowFilterBt(ObjRef(schema, table), col, value)
      case other => throw new UnsupportedOperationException(s"RowFilter kind $other")
    }
  }
}

case class ColumnRowFilterIn(table: ObjRef, columnName: String, values: Set[Any]) extends RowFilter {
  override def accept(row: ValueSet): Boolean = {
    row.get(columnName) match {
      case None => false
      case Some(v) => values(v)
    }
  }
}

case class ColumnRowFilterBt(table: ObjRef, columnName: String, threshold: Any) extends RowFilter {
  override def accept(row: ValueSet): Boolean = {
    row.get(columnName) match {
      case None => false
      case Some(value) => (value, threshold) match  {
        case (v: Long, t: Long)  => v > t
      }
    }
  }
}
