package scamr.io

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream, ByteArrayOutputStream}
import java.nio.charset.Charset
import org.apache.commons.codec.binary.Base64


class SerializableFunction2[-T1, -T2, +R](private val lambda: (T1, T2) => R) extends Function2[T1, T2, R] with Serializable {
  override def apply(a: T1, b: T2): R = lambda(a, b)
}

object SerializableFunction2 {
  private val charset = Charset.forName("ISO-8859-1")

  def serializeToBase64String[T1, T2, R](function: SerializableFunction2[T1, T2, R]): String = {
    val bytesOut = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytesOut)
    out.writeObject(function)
    out.close()
    new String(Base64.encodeBase64(bytesOut.toByteArray), charset)
  }

  def deserializeFromBase64String[T1, T2, R](string: String): SerializableFunction2[T1, T2, R] = {
    val in = new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(string.getBytes(charset))))
    val result = in.readObject()
    result.asInstanceOf[SerializableFunction2[T1, T2, R]]
  }
}
