package scamr.conf

import org.apache.hadoop.conf.Configuration

object HadoopVersionSpecific {
  import scamr.Version.{Cdh3, Cdh4}

  def isNativeSnappyLoaded(conf: Configuration): Boolean = {
    try {
      val snappyCodecClass = java.lang.Class.forName("org.apache.hadoop.io.compress.SnappyCodec")
      val isLoaded = if (scamr.Version.isCdh3) {
        val method = snappyCodecClass.getDeclaredMethod("isNativeSnappyLoaded", classOf[Configuration])
        method.invoke(null, conf).asInstanceOf[Boolean]
      } else if (scamr.Version.isCdh4) {
        val method = snappyCodecClass.getDeclaredMethod("isNativeCodeLoaded")
        method.invoke(null).asInstanceOf[Boolean]
      } else {
        // Some other hadoop version, don't know how to check if Snappy is loaded, assume false.
        false
      }
      isLoaded
    } catch {
      // SnappyCodec class not found, assume false.
      case e: ClassNotFoundException => false
    }
  }

  // Hadoop-version specific configuration keys for parameters that ScaMR framework sets.
  object ConfKeys {
    // Speculative execution
    lazy val SpeculativeMappers: String = scamr.Version match {
      case Cdh3() => "mapred.map.tasks.speculative.execution"
      case Cdh4() => "mapreduce.map.speculative"
    }

    lazy val SpeculativeReducers: String = scamr.Version match {
      case Cdh3() => "mapred.reduce.tasks.speculative.execution"
      case Cdh4() => "mapreduce.reduce.speculative"
    }

    // Default FS, used by -Dscamr.local.mode
    lazy val DefaultFilesystem: String = scamr.Version match {
      case Cdh3() => "fs.default.name"
      case Cdh4() => "fs.defaultFS"
    }

    // Output compression settings, used by InputOutput.FileLink
    lazy val CompressOutput: String = scamr.Version match {
      case Cdh3() => "mapred.output.compress"
      case Cdh4() => "mapreduce.output.fileoutputformat.compress"
    }
    lazy val OutputCompressionType: String = scamr.Version match {
      case Cdh3() => "mapred.output.compression.type"
      case Cdh4() => "mapreduce.output.fileoutputformat.compress.type"
    }
    lazy val OutputCompressionCodec: String = scamr.Version match {
      case Cdh3() => "mapred.output.compression.codec"
      case Cdh4() => "mapreduce.output.fileoutputformat.compress.codec"
    }

    // Intermediate compression settings, used by MapReduceJob
    lazy val CompressMapOutput: String = scamr.Version match {
      case Cdh3() => "mapred.compress.map.output"
      case Cdh4() => "mapreduce.map.output.compress"
    }
    lazy val MapOutputCompressionType: String = scamr.Version match {
      case Cdh3() => "mapred.map.output.compression.type"
      case Cdh4() => "mapred.map.output.compression.type"  // Note: looks like they forgot to update this one?
    }
    lazy val MapOutputCompressionCodec: String = scamr.Version match {
      case Cdh3() => "mapred.map.output.compression.codec"
      case Cdh4() => "mapreduce.map.output.compress.codec"
    }

  }
}
