package scamr.io.lib

import java.io.{DataInput, DataOutput}
import org.apache.hadoop.io.WritableComparable

/**
 * A 2-tuple representing a file name and byte offset within the file. Used as keys by
 * {@link CombineTextFileInputFormat}.
 *
 * @param fileName the file name
 * @param offset byte offset within the file
 */
case class FileNameWithOffset(var fileName: String, var offset: Long) extends WritableComparable[FileNameWithOffset] {
  def this() = this("", 0L)

  override def write(out: DataOutput) {
    out.writeLong(offset)
    out.writeUTF(fileName)
  }

  override def readFields(in: DataInput) {
    offset = in.readLong()
    fileName = in.readUTF()
  }

  override def compareTo(that: FileNameWithOffset): Int = this.fileName.compareTo(that.fileName) match {
    case 0 => if (this.offset < that.offset) -1 else if (this.offset > that.offset) 1 else 0
    case nonZero => nonZero
  }
}
