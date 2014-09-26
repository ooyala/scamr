package scamr.mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, FileSystem, Path}
import org.apache.hadoop.mapreduce.{Counter, CounterGroup, Job}
import scamr.conf.{ConfOrJobModifier, JobModifier, ConfModifier}
import scamr.io.InputOutput.FileLink
import scamr.io.{InputOutputUtils, InputOutput}
import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import com.lambdaworks.jacks.JacksMapper
import scala.collection.mutable
import scamr.mapreduce.util.MetadataWriter

class MapReducePipeline(protected val pipeline: MapReducePipeline.PublicExecutable) {
  def execute(): Boolean = pipeline.execute()
}

object MapReducePipeline {

  import scala.collection.JavaConversions._

  trait Stage[K1, V1, K2, V2] {
    val prev: Stage[_, _, _ <: K1, V1]
    var next: Stage[_ >: K2, V2, _, _] = _

    val baseConfiguration: Configuration
    def execute(): Boolean
  }

  trait SourceLike[K, V] {
    val source: InputOutput.Source[K, V]
  }

  trait SinkLike[K, V] {
    val sink: InputOutput.Sink[K, V]
  }

  trait JobLike[K1, V1, K2, V2] {
    val scamrJob: MapReduceJob[K1, V1, _, _, K2, V2]
  }

  trait PublicExecutable {
    def execute(): Boolean
  }

  // Initializes a new pipeline
  def init(baseConfiguration: Configuration): InitialStage = new InitialStage(baseConfiguration)
  def init(): InitialStage = new InitialStage(new Configuration)

  // The only type that can be chained from an InitialStage is an InputOutput.Source which returns an
  // InputStage.
  class InitialStage(override val baseConfiguration: Configuration)
  extends Stage[None.type, None.type, None.type, None.type] {
    override val prev = null
    this.next = null

    def -->[K, V](source: InputOutput.Source[K, V]): InputStage[K, V] = {
      val nextStage = new InputStage[K, V](this, source)
      this.next = nextStage
      nextStage
    }

    // Only JobStages really execute, everyone else just recurses to the previous stage in the chain.
    // The InitialStage always returns true.
    override def execute(): Boolean = true
  }

  // The only type that can be chained from an InputStage is a JobStage that defines a MapReduce job which
  // processes the input.
  class InputStage[K, V](override val prev: InitialStage, override val source: InputOutput.Source[K, V])
  extends Stage[None.type, None.type, K, V] with SourceLike[K, V] {
    override val baseConfiguration = prev.baseConfiguration

    def -->[K1 >: K, K2, V2](job: MapReduceJob[K1, V, _, _, K2, V2])
                            (implicit k2m: Manifest[K2], v2m: Manifest[V2]): JobStage[K1, V, K2, V2] = {
      val nextStage = new JobStage[K1, V, K2, V2](this, job)(k2m, v2m)
      this.next = nextStage
      nextStage
    }

    // Only JobStages really execute, everyone else just recurses to the previous stage in the chain
    override def execute(): Boolean = prev.execute()
  }

  // Two types can be chained from a JobStage which return a new stage:
  //   chaining an InputOutput.Sink returns an OutputStage and terminates the pipeline
  //   chaining another MapReduceJob returns a new JobStage and creates a multi-stage job pipeline
  //
  // Additionally, ConfModifiers or JobModifiers can be chained from a JobStage, these return the
  // same stage but modify the list of conf/job modifiers.
  class JobStage[K1, V1, K2, V2](override val prev: Stage[_, _, _ <: K1, V1],
                                 override val scamrJob: MapReduceJob[K1, V1, _, _, K2, V2])
                                (implicit val k2m: Manifest[K2], v2m: Manifest[V2])
  extends Stage[K1, V1, K2, V2] with JobLike[K1, V1, K2, V2] {
    val logger = Logger.getLogger(this.getClass)

    override val baseConfiguration = prev.baseConfiguration

    protected var confModifiers: List[ConfModifier] = List()
    protected var jobModifiers: List[JobModifier] = List()

    val workingDir: Path = InputOutputUtils.randomWorkingDir(new Path("tmp"), scamrJob.name)

    def -->(sink: InputOutput.Sink[K2, V2]): MapReducePipeline = {
      val nextStage = new OutputStage[K2, V2](this, sink)
      this.next = nextStage
      new MapReducePipeline(nextStage)
    }

    // TODO(ivmaykov): Try to come up with a cleaner way to add sinks while inferring the k/v types than
    // wrapping them in a 0-ary closure.
    def -->(sinkGenerator: () => InputOutput.Sink[K2, V2]): MapReducePipeline = this --> sinkGenerator()

    def -->[K3, V3](nextJob: MapReduceJob[K2, V2, _, _, K3, V3], link: Option[FileLink[K2, V2]] = None)
                   (implicit k3m: Manifest[K3], v3m: Manifest[V3]): JobStage[K2, V2, K3, V3] = {
      val nextStage = new LinkStage[K2, V2](this, workingDir, link)(k2m, v2m)
      this.next = nextStage
      nextStage --> nextJob
    }

    def ++(confModifier: ConfModifier): JobStage[K1, V1, K2, V2] = {
      confModifiers = confModifier :: confModifiers
      this
    }

    def ++(jobModifier: JobModifier): JobStage[K1, V1, K2, V2] = {
      jobModifiers = jobModifier :: jobModifiers
      this
    }

    def ++(modifiers: Iterable[_ <: ConfOrJobModifier]): JobStage[K1, V1, K2, V2] = {
      modifiers.foreach { modifier: ConfOrJobModifier =>
        modifier match {
          case confModifier: ConfModifier => confModifiers = confModifier :: confModifiers
          case jobModifier: JobModifier => jobModifiers = jobModifier :: jobModifiers
          case _ => throw new IllegalArgumentException("Invalid modifier class: " + modifier.getClass.toString)
        }
      }
      this
    }

    override def execute(): Boolean = {
      var result = prev.execute()
      // TODO(ivmaykov): Throw an exception?
      if (!result) return false

      val job = createAndConfigureJob
      result = job.waitForCompletion(true)

      // Keep a copy of the job history in the target directory
      if (result && job.getConfiguration.getBoolean("scamr.metadata.write", false)) {
        MetadataWriter.writeMetadata(job)
      }

      // Tell our Source that the input has been read, and whether we succeeded or not
      // Tell our Sink that the output has been written, and whether we succeeded or not
      prev.asInstanceOf[SourceLike[K1, V1]].source.onInputRead(job, result)
      next.asInstanceOf[SinkLike[K2, V2]].sink.onOutputWritten(job, result)
      result
    }

    // Configures this stage
    protected def createAndConfigureJob: Job = {
      // Note: Creating a new Job copies the baseConfiguration. Make sure to use job.getConfiguration from
      // this point on!
      val hadoopJob = new Job(baseConfiguration, scamrJob.name)
      hadoopJob.setJarByClass(scamrJob.mapperClass)
      confModifiers.foreach { _.apply(hadoopJob.getConfiguration) }
      jobModifiers.foreach { _.apply(hadoopJob) }
      prev.asInstanceOf[SourceLike[K1, V1]].source.configureInput(hadoopJob)
      next.asInstanceOf[SinkLike[K2, V2]].sink.configureOutput(hadoopJob)
      scamrJob.configureJob(hadoopJob)
      hadoopJob
    }
  }

  class LinkStage[K, V](previous: Stage[_, _, K, V], val workingDir: Path, fileLink: Option[FileLink[K, V]] = None)
                       (implicit km: Manifest[K], vm: Manifest[V])
  extends Stage[K, V, K, V] with SourceLike[K, V] with SinkLike[K, V] {

    override val prev = previous.asInstanceOf[Stage[_, _, _ <: K, V]]
    private val link: FileLink[K, V] = fileLink match {
      case None => new InputOutput.SequenceFileLink[K, V](workingDir)
      case Some(l) => l
    }
    override val sink: InputOutput.Sink[K, V] = link
    override val source: InputOutput.Source[K, V] = link
    override val baseConfiguration = prev.baseConfiguration

    def -->[K1 >: K, K2, V2](job: MapReduceJob[K1, V, _, _, K2, V2])
                            (implicit k2m: Manifest[K2], v2m: Manifest[V2]): JobStage[K1, V, K2, V2] = {
      val nextStage = new JobStage[K1, V, K2, V2](this, job)(k2m, v2m)
      this.next = nextStage
      nextStage
    }

    // Only JobStages really execute, everyone else just recurses to the previous stage in the chain
    override def execute(): Boolean = prev.execute()

    def cleanupWorkingDir(conf: Configuration) { link.cleanupWorkingDir(conf) }
  }

  class OutputStage[K, V](previous: Stage[_, _, K, V], override val sink: InputOutput.Sink[K, V])
  extends Stage[K, V, None.type, None.type] with SinkLike[K, V] with PublicExecutable {
    override val prev = previous.asInstanceOf[Stage[_, _, _ <: K, V]]
    override val baseConfiguration = prev.baseConfiguration

    // Only JobStages really execute, everyone else just recurses to the previous stage in the chain
    override def execute(): Boolean = prev.execute()
  }
}
