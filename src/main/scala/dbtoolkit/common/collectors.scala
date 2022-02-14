package dbtoolkit.common

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.{Connection, SQLException}

abstract class Collector[T] {
  def apply(t: T): Unit
  def close(): Unit
}

trait CountingCollector[T] extends Collector[T] {
  private var _count = 0

  def count: Int = _count

  abstract override def apply(t: T): Unit = {
    super.apply(t)
    _count = _count + 1
  }
}

class ToFileCollector[T](file: File, lineDecorator: T => String = { e: T => e.toString }) extends Collector[T] {
  file.getParentFile.mkdirs()
  val out = new BufferedWriter(new FileWriter(file))

  override def apply(t: T): Unit = {
    out.write(lineDecorator(t))
    out.newLine()
    if (System.currentTimeMillis() % 10 == 0) out.flush()
  }

  override def close(): Unit = out.close()
}

class ExecutingCollector(con: Connection, lenient: Boolean) extends Collector[String] {
  val stat = con.createStatement()

  override def apply(t: String): Unit = {
    println(t)
    try {
      stat.executeUpdate(t)
    } catch {
      case ex: SQLException => lenient match {
        case true => println(s"Skipped. ERROR: ${ex.getMessage}")
        case false => throw ex
      }
    }
  }

  override def close(): Unit = stat.close()
}

class NoCollector[T] extends Collector[T] {
  override def apply(t: T): Unit = println(t)
  override def close(): Unit = {}
}

object Collector {

  def printlnCollector: CountingCollector[String] = new NoCollector[String] with CountingCollector[String]

  def sqlFileCollector(file: File): CountingCollector[String] =
    new ToFileCollector[String](file, { e => s"$e;" } ) with CountingCollector[String]

  def executingCollector(con: Connection, lenient: Boolean): CountingCollector[String] =
    new ExecutingCollector(con, lenient) with CountingCollector[String]

}
