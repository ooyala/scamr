package scamr.conf

import scala.util.matching.Regex
import java.util.regex.Pattern
import org.apache.hadoop.conf.Configuration

object SetConfigParam {
  // Use this for a single config param
  def apply[T](name: String, value: T) = LambdaConfModifier { setConfigParam(_, name, value) }

  // Use this for a series of config params which can be specified with Map notation:
  //   SetConfigParam(name1 -> value1, name2 -> value2, name3 -> value3)
  def apply(firstPair: (String, Any), extraPairs: (String, Any)*) = LambdaConfModifier {
    conf => {
      setConfigParam(conf, firstPair._1, firstPair._2)
      extraPairs.foreach { pair => setConfigParam(conf, pair._1, pair._2) }
    }
  }

  private def setConfigParam(conf: Configuration, name: String, value: Any) {
    value match {
      case string: String => conf.set(name, string)
      case int: Int => conf.setInt(name, int)
      case long: Long => conf.setLong(name, long)
      case float: Float => conf.setFloat(name, float)
      case boolean: Boolean => conf.setBoolean(name, boolean)
      case pattern: Pattern => conf.setPattern(name, pattern)
      case regex: Regex => conf.setPattern(name, regex.pattern)
    }
  }
}