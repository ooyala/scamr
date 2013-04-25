package scamr.io

import java.io.{OutputStream, InputStream}
import org.apache.hadoop.io.compress._

/**
 * A compression codec class that's never meant to be actually used for anything. All method implementations
 * throw UnsupportedOperationExceptions. This is meant to be used as a special value for the
 * "scamr.intermediate.compression.codec" config parameter to signify that intermediate compression should
 * not be used. To disable ScaMR's intermediate compression, add the following to your MR command line:
 *   -Dscamr.intermediate.compression.codec=scamr.io.NullCompressionCodec
 */
class NullCompressionCodec extends CompressionCodec {
  def createOutputStream(p1: OutputStream): CompressionOutputStream = notImplemented("createOutputStream")

  def createOutputStream(p1: OutputStream, p2: Compressor): CompressionOutputStream =
    notImplemented("createOutputStream")

  def getCompressorType: Class[_ <: Compressor] = notImplemented("getCompressorType")

  def createCompressor(): Compressor = notImplemented("createCompressor")

  def createInputStream(p1: InputStream): CompressionInputStream = notImplemented("createInputStream")

  def createInputStream(p1: InputStream, p2: Decompressor): CompressionInputStream =
    notImplemented("createInputStream")

  def getDecompressorType: Class[_ <: Decompressor] = notImplemented("getDecompressorType")

  def createDecompressor(): Decompressor = notImplemented("createDecompressor")

  def getDefaultExtension: String = notImplemented("getDefaultExtension")

  private def notImplemented(methodName: String) =
    throw new UnsupportedOperationException("%s not implemented!".format(methodName))
}
