package scamr.mapreduce.lib

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.MapContext
import scamr.mapreduce.mapper.SimpleMapper

/**
 * This version of TextInputMapper only works with the standard TextInputFormat. It does not work with
 * the CombineTextFileInputFormat, which has a different key type (FileNameWithOffset vs. LongWritable).
 * Since most mappers wouldn't actually care that much about the offsets, and would only care about the
 * body of the file, it's recommended to subclass the AnyKeyTextInputMapper instead. That mapper will
 * work with either input format out of the box.
 */
@deprecated("Prefer IgnoreKeyTextInputMapper instead", "2013-07-29")
abstract class TextInputMapper[K2, V2](context: MapContext[_, _, _, _])
extends SimpleMapper[LongWritable, Text, K2, V2](context)

/**
 * A base mapper type that processes inputs where keys are anything (and are ignored), and values are
 * lines of text.
 */
abstract class AnyKeyTextInputMapper[K2, V2](context: MapContext[_, _, _, _])
extends SimpleMapper[Any, Text, K2, V2](context)
