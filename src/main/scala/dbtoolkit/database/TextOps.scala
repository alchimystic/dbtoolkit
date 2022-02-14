package dbtoolkit.database

/**
  * Helper for manipulating text
  */
object TextOps {

  def underscoredToCamelCase(value: String): String = {
    def _conv(chars: List[Char]): String = chars match {
      case Nil => ""
      case '_' :: ch :: tail => ch.toUpper.toString + _conv(tail)
      case head :: tail => head.toString + _conv(tail)
    }
    _conv(value.toList)
  }

 def upper(source: String): String = source.toList match {
    case Nil => source
    case head :: tail => head.toUpper + tail.mkString
  }

  def lower(source: String): String = source.toList match {
    case Nil => source
    case head :: tail => head.toLower + tail.mkString
  }

  def entityClassName(name: String): String = upper(underscoredToCamelCase(name))
}
