package scamr.io.lib

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, CombineFileRecordReader, CombineFileInputFormat}
import org.apache.hadoop.mapreduce.{JobContext, RecordReader, TaskAttemptContext, InputSplit}
import org.apache.hadoop.util.LineReader

/**
 * An implementation of {@link CombineFileInputFormat} which works for compressed or uncompressed text files,
 * similar to TextInputFormat. Use for processing many small text files within a single Mapper.
 *
 * The keys are {@link FileNameWithOffset}, which is a 2-tuple of the source file name and byte offset within
 * the file. The values are {@link Text}.
 *
 * Note: by default, all input files end up in a single input split and thus spawn a single Mapper. If your input
 * is not trivially small, this may take a while! The easiest way to control this behavior is to set the
 * "mapred.max.split.size" conf property. When this value is non-0, files will be grouped into a single
 * split as long as the total size of all files doesn't exceed the max split size.
 *
 * For example: passing -Dmapred.max.split.size=67108864 to your MR job will create multi-file splits such that the
 * combined size of all files in any split is <= 64 MB.
 */
class CombineTextFileInputFormat extends CombineFileInputFormat[FileNameWithOffset, Text] {
  override def createRecordReader(split: InputSplit,
                                  context: TaskAttemptContext): RecordReader[FileNameWithOffset, Text] =
    new CombineFileRecordReader(split.asInstanceOf[CombineFileSplit], context, classOf[CombineFileLineRecordReader])

  override def isSplitable(context: JobContext, filename: Path): Boolean =
    new CompressionCodecFactory(context.getConfiguration).getCodec(filename) == null
}

/** The record reader used by {@link CombineTextFileInputFormat}. */
class CombineFileLineRecordReader(inputSplit: CombineFileSplit, context: TaskAttemptContext, index: java.lang.Integer)
extends RecordReader[FileNameWithOffset, Text] {

  protected val conf = context.getConfiguration
  protected val path = inputSplit.getPath(index)
  protected val compressionCodec = Option(new CompressionCodecFactory(conf).getCodec(path))
  // offset of the chunk
  protected var startOffset = inputSplit.getOffset(index)
  // end of the chunk
  protected val end = if (compressionCodec.isDefined) Long.MaxValue else startOffset + inputSplit.getLength(index)
  // current position
  protected var pos = startOffset

  require(!compressionCodec.isDefined || startOffset == 0, "Cannot have a non-0 offset into a compressed file!")

  // if starting partway through the file, go back 1 byte but skip the first partial line we read
  protected val skipFirstLine = startOffset != 0
  if (skipFirstLine) {
    startOffset -= 1
    pos = startOffset
  }

  protected val key = new FileNameWithOffset(path.toString, startOffset)
  protected val value = new Text

  protected var reader: LineReader = null

  override def close(): Unit = if (reader != null) {
    reader.close()
    reader = null
  }

  override def initialize(split: InputSplit, ctx: TaskAttemptContext) {
    assert(split == this.inputSplit, "Wrong input split!")
    val fs = path.getFileSystem(conf)
    val fileIn = compressionCodec match {
      case Some(codec) => codec.createInputStream(fs.open(path))
      case None => fs.open(path)
    }
    if (startOffset != 0) {
      fileIn.seek(startOffset)
    }

    val recordDelimiterBytes: Option[Array[Byte]] = Option(conf.get("textinputformat.record.delimiter")) match {
      case None => None
      case Some(delimeterString) => Some(delimeterString.getBytes)
    }
    reader = recordDelimiterBytes match {
      case Some(bytes) => new LineReader(fileIn, conf, bytes)
      case None => new LineReader(fileIn, conf)
    }
    if (skipFirstLine) {
      startOffset += reader.readLine(new Text(), 0, math.min(Int.MaxValue.toLong, end - startOffset).toInt)
      pos = startOffset
    }
  }

  override def nextKeyValue(): Boolean = if (pos < end) {
    key.offset = pos
    val newSize = reader.readLine(value)
    pos += newSize
    newSize != 0  // if this is 0, we hit EOF
  } else {
    false
  }

  override def getCurrentKey: FileNameWithOffset = key

  override def getCurrentValue: Text = value

  override def getProgress: Float =
    if (startOffset == end) 0.0f else math.min(1.0f, (pos - startOffset).toFloat / (end - startOffset))
}
