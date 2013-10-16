package scamr

import scala.util.matching.Regex

trait Version {
  val versionString: String
}

trait VersionGroup {
  val regex: Regex

  def unapply(v: Version): Boolean = v.versionString match {
    case regex() => true
    case _ => false
  }
}

/**
 * Object representing the current ScaMR version. Usage:
 *   scamr.Version() or scamr.Version.versionString - returns the full version string
 *
 * Branching your code depending on the CDH version:
 *   scamr.Version match {
 *     case scamr.Version.Cdh3() =>  // ... CDH-3 specific code
 *     case scamr.Version.Cdh4() =>  // ... CDH-4 specific code
 *   }
 *
 * or alternatively:
 *
 *   if (scamr.Version.isCdh3) {
 *     // ... CDH-3 specific code
 *   } else if (scamr.Version.isCdh4) {
 *     // ... CDH-4 specific code
 *   }
 */
object Version extends Version {
  // TODO(ivmaykov): Figure out if it's possible to auto-generate this file from build.sbt or version.sbt
  // Specifically, this value needs to be set by the sbt-release plugin and everything else should remain as-is.
  override val versionString = "0.3.1-cdh4-SNAPSHOT"

  def apply(): String = versionString
  override def toString = versionString

  // Predefined version groups for CDH3 and CDH4
  case object Cdh3 extends VersionGroup { val regex = ".*cdh3.*".r }
  case object Cdh4 extends VersionGroup { val regex = ".*cdh4.*".r }

  val (isCdh3, isCdh4): (Boolean, Boolean) = this match {
    case Cdh3() => (true, false)
    case Cdh4() => (false, true)
  }

  if (!isCdh3 && !isCdh4) {
    throw new RuntimeException("ScaMR version is neither of CDH3 or CDH4: " + versionString)
  }
}
