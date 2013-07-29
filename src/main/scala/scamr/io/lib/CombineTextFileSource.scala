package scamr.io.lib

import org.apache.hadoop.io.Text
import scamr.io.InputOutput

/**
 * A ScaMR {@link InputOutput.Source} for processing files with {@link CombineTextFileInputFormat}.
 * Use for processing many small text files in a single mapper, rather than one mapper per file.
 * Note: see the comment about split sizes in the documentation of {@link CombineTextFileInputFormat}.
 *
 * @param inputs one or more file input patterns
 */
class CombineTextFileSource(val inputs: Iterable[String])
extends InputOutput.FileSource[FileNameWithOffset, Text](classOf[CombineTextFileInputFormat], inputs)
