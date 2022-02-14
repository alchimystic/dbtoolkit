package dbtoolkit.common

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import dbtoolkit.database.dialect.Dialect
import configs.syntax._
import dbtoolkit.database._

import scala.collection.JavaConverters._

class ToolConfig(config: Config) {

  lazy val dialect: Dialect = Dialect(config.getString("dialect"))

  lazy val schema = config.get[String]("schema").toOption

  lazy val skipTables: Set[ObjRef] = config.get[List[String]]("skipTables").value.map(ObjRef(schema, _)).toSet

  lazy val initialTable: Option[ObjRef] = {
    Option(config.get[String]("initialTable").value).map(ObjRef(schema, _))
  }

  lazy val referenceTables: List[ObjRef] = config.get[List[String]]("referenceTables").value.map(ObjRef(schema, _))

  lazy val tables: List[ObjRef] = config.get[List[String]]("tables").value.map(ObjRef(schema, _))

  lazy val pkExcludes: Set[String] = config.get[List[String]]("pkExcludes").value.toSet

  lazy val dataTransformation: DataTransformation = {
    val root = config.getConfig("dataTransformation")

    val allRules = getConfigKeys(root).map { tablename =>
      val cfg = root.getConfig(tablename)
      val ref = ObjRef(schema, tablename)
      val cols = getConfigKeys(cfg)
      val rules = cols.map { col => col -> cfg.getString(col) }.toMap
      tablename -> rules
    }

    val parts = allRules.partition(_._1 == "common")
    val common: Map[String, String] = parts._1 .headOption.map(_._2).getOrElse(Map.empty)
    val transformers = parts._2.map { entry =>
      val ref = ObjRef(schema, entry._1)
      ref -> ValueTransformers.valueSetTransformer(entry._2, common)
    }.toMap
    DataTransformation(transformers, ValueTransformers.valueSetTransformer(common))
  }

  lazy val tableFilters: TableFilters = {
    val root = config.getConfig("tableFilters")
    val map = getConfigKeys(root).map { tablename =>
      val obj = ObjRef(schema, tablename)
      obj -> root.getString(tablename)
    }.toMap

    TableFilters(map)
  }

  lazy val customKeys: List[Key] =  config.getConfigList("customKeys").asScala.toList.map(tableKey(_))

  private def getConfigKeys(cfg: Config): Set[String] = cfg.root().keySet().asScala.toSet

  def dbSettings(key: String): DatabaseSettings = {
    val path = config.resolve().get[String]("credentialsPath").value
    val cfg = ConfigFactory.parseFile(new File(path)).getConfig(key)
    val url = cfg.get[String]("url").value
    val username = cfg.get[String]("username").value
    val pwd = cfg.get[String]("password").value
    val ro = cfg.get[Boolean]("readOnly").toOption.getOrElse(true)
    val schem = cfg.get[String]("schema").toOption.orElse(schema)
    DatabaseSettings(dialect, url, username, pwd, ro, schem)
  }

  private def keyLink(cfg: Config): KeyLink = {
    val pk = tableKey(cfg.getConfig("pk"))
    val fk = tableKey(cfg.getConfig("fk"))
    val follow = cfg.get[String]("follow").toOption.map(_.toBoolean).getOrElse(true)
    val name = s"CustomLink_${pk.tableRef.name}_${fk.tableRef.name}"
    KeyLink(name, pk, fk, follow)
  }

  private def tableKey(cfg: Config): Key = {
    val table = cfg.get[String]("table").value
    val cols = cfg.get[List[String]]("cols").value
    Key(ObjRef(schema, table), cols)
  }

  lazy val customLinks: List[KeyLink] = config.getConfigList("customLinks").asScala.toList.map(keyLink(_))

  lazy val rowFilters: List[RowFilter] = config.getConfigList("rowFilters").asScala.toList.map(RowFilter(_, schema))

}

object ToolConfig {

  def apply(file: File): ToolConfig = new ToolConfig(ConfigFactory.parseFile(file))

}
