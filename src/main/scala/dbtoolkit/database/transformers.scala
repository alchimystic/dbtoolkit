package dbtoolkit.database

import java.security.MessageDigest

import DataTool.ValueSet

import scala.util.{Failure, Success, Try}

/**
  * Relative to transformations of rows (ValueSetTransformer) or column values (ValueTransformer)
  */
object ValueTransformers {

  //represents a transformation of a value, based on its row (ValueSet)
  //None means 'remove value'. Internally the Any can either be the directly the new value, or None to keep the original
  type ValueTransformer = ValueSet => Option[Any]

  //represents a transformation of a value set
  type ValueSetTransformer = ValueSet => ValueSet

  val noTransform: ValueSetTransformer = { set => set } //no transformation

  // None means remove value
  val remove: ValueTransformer = { _ => None }

  // keeps the value: the outer Some means 'have a value', inner None means 'dont transform'
  val keepValue: ValueTransformer = { _ => Some(None) }

  private val copyExpr = "copy:(.*)".r
  private val fixedExpr = "fixed:(.*)".r
  private val customExpr = "custom:(.*)".r
  private val hashwrapExpr = "hashwrap:(.*)".r
  private val typedValueExpr = "(.*)@(.*)".r
  private val hashplateExpr = "hashplate:(.*)".r
  private val formatExpr = "format:(.*):(.*)".r

  /**
    * @param column
    * @return transformer which copies the value of another column
    */
  def copy(column: String): ValueTransformer = { valueSet => valueSet.get(column) }

  /**
    * @param value fixed value to return in transformer
    * @return transformer which returns a fixed value
    */
  def fixed(value: String): ValueTransformer = value match {
    case typedValueExpr(tp, subValue) =>
      val resultValue = tp match {
        case "Int" => subValue.toInt
        case other => throw new UnsupportedOperationException(s"Fixed value for type $other")
      }
    { _ => Some(resultValue) }
    case _ => { _ => Some(value) }
  }

  def hashWrap(column: String): ValueTransformer = { valueSet =>
    valueSet.get(column).map(value =>
      Option(value).map(v =>
        s"Some $column ${ hashOf(v)}"
      ).getOrElse(null)
    )
  }

  def hashPlate(column: String): ValueTransformer = { valueSet =>
    valueSet.get(column).map(value =>
      Option(value).map { v =>
        val hash = hashOf(v).take(6)
        s"${hash.substring(0,2)}-${hash.substring(2,4)}-${hash.substring(4,6)}".toUpperCase()
      }.getOrElse(null)
    )
  }

  def formatTransformer(column: String, pattern: String): ValueTransformer = { valueSet =>
    valueSet.get(column).map(value =>
      Option(value).map { v =>
        String.format(pattern, value.asInstanceOf[Object])
      }.getOrElse(null)
    )
  }

  private def hashOf(value: Any): String = {
    val bytes = MessageDigest.getInstance("MD5").digest(value.toString.getBytes)
    bytes.map( b => String.format("%02x", Byte.box(b))).mkString("")
  }

  /**
    * @param className
    * @return transformer based on a ValueTransformer impl classname
    */
  def custom(className: String): ValueTransformer = {
    Class.forName(className).asInstanceOf[Class[ValueTransformer]].newInstance()
  }

  /**
    * @param str
    * @return attempted ValueTransformer from a definition string
    */
  def apply(str: String): Try[ValueTransformer] = transformerOf(str)

  def transformerOf(str: String): Try[ValueTransformer] = {
    try {
      Success(str match {
        case "remove" => remove
        case "keep" => keepValue
        case copyExpr(column) => copy(column.trim)
        case fixedExpr(value) => fixed(value.trim)
        case hashwrapExpr(column) => hashWrap(column.trim)
        case hashplateExpr(column) => hashPlate(column.trim)
        case formatExpr(column, pattern) => formatTransformer(column, pattern)
        case customExpr(classname) => custom(classname.trim)
      })
    } catch {
      case ex: Exception => Failure(ex)
    }
  }

  /**
    * @param rules map of column -> rule definition string
    * @return ValueSetTransformer with all the individual column transformers
    */
  def valueSetTransformer(rules: Map[String, String]): ValueSetTransformer = {
    new SimpleValueSetTransformer(rules);
  }

  def valueSetTransformer(primary: Map[String, String], secondary: Map[String, String]): ValueSetTransformer = {
    val keys = primary.keySet
    val extra = secondary.filter(e => !keys(e._1))
    valueSetTransformer(primary ++ extra)
  }

  class SimpleValueSetTransformer(rules: Map[String, String]) extends ValueSetTransformer {
    val parts = rules.mapValues( transformerOf(_).get )
    override def apply(input: ValueSet): ValueSet = {
      input.map {
        case (col, value) =>
          val part = parts.get(col)
          val newValue = part match {
            case None => value
            case Some(trans) if trans == keepValue => value
            case Some(trans) => trans(input).getOrElse(null)
          }
          //          parts.get(col).map( _(input).getOrElse(null) ).getOrElse(value)
          col -> newValue
      }
    }
  }
}
