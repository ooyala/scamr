package scamr.mapreduce

import org.apache.hadoop.io.compress.{DefaultCodec, CompressionCodec, SnappyCodec}
import org.apache.hadoop.mapreduce.{Reducer, Mapper, Job}
import scamr.conf.{HadoopVersionSpecific, ConfModifier, LambdaConfModifier}
import scamr.io.NullCompressionCodec
import scamr.mapreduce.combiner.CombinerDef
import scamr.mapreduce.mapper.MapperDef
import scamr.mapreduce.reducer.ReducerDef

// Note the private primary c-tor. Class can only be instantiated via secondary constructors which take
// {Mapper|Combiner|Reducer}Def parameters.
// K1, V1 - key/value types for mapper input
// K2, V2 - intermediate key/value types
// K3, V3 - key/value types for reducer output
class MapReduceJob[K1, V1, K2, V2, K3, V3] protected
    (val mapperClass: Class[_ <: Mapper[K1, V1, K2, V2]],
     val combinerClass: Option[Class[_ <: Reducer[K2, V2, K2, V2]]],
     val reducerClass: Option[Class[_ <: Reducer[K2, V2, K3, V3]]],
     val name: String,
     confMods: List[ConfModifier] = List())
    (implicit val mapOutputKeyType: Manifest[K2],
     val mapOutputValueType: Manifest[V2],
     val outputKeyType: Manifest[K3],
     val outputValueType: Manifest[V3]) {

  require(mapperClass != null, "Not allowed: null mapper class!")
  require(combinerClass != null, "Not allowed: null combiner class!")
  require(reducerClass != null, "Not allowed: null reducer class!")
  require(combinerClass == None || reducerClass != None, "Can't have a combiner without a reducer!")
  require(name != null && !name.isEmpty, "Not allowed: null or empty job name!")

  val confModifiers = if (reducerClass != None) {
    LambdaConfModifier { conf =>
      val defaultCodec = if (HadoopVersionSpecific.isNativeSnappyLoaded(conf)) {
        // Prefer the SnappyCodec if the Snappy native libraries are loaded ...
        classOf[SnappyCodec]
      } else {
        // ... else, prefer LzoCodec if the GPL'ed hadoop-gpl-compression library is installed on our cluster,
        // and fall back to DefaultCodec if neither of Snappy or Lzo is available.
        Option(conf.getClass("io.compression.codec.lzo.class", null, classOf[CompressionCodec])) match {
          case Some(lzoCodec) => lzoCodec
          case None => classOf[DefaultCodec]
        }
      }
      // Allow the user to overwrite the codec we actually use on the command line
      val codecClass = conf.getClass("scamr.intermediate.compression.codec", defaultCodec, classOf[CompressionCodec])

      if (codecClass != classOf[NullCompressionCodec]) {
        conf.setBoolean(HadoopVersionSpecific.ConfKeys.CompressMapOutput, true)
        conf.set(HadoopVersionSpecific.ConfKeys.MapOutputCompressionType, "BLOCK")
        conf.setClass(HadoopVersionSpecific.ConfKeys.MapOutputCompressionCodec, codecClass, classOf[CompressionCodec])
      } else {
        conf.setBoolean("mapred.compress.map.output", false)
      }
    } :: confMods
  } else {
    confMods
  }

  def configureJob(job: Job) {
    job.setMapperClass(mapperClass)

    if (combinerClass != None) {
      job.setCombinerClass(combinerClass.get)
    }

    if (reducerClass != None) {
      job.setReducerClass(reducerClass.get)
      job.setMapOutputKeyClass(mapOutputKeyType.erasure.asInstanceOf[Class[K2]])
      job.setMapOutputValueClass(mapOutputValueType.erasure.asInstanceOf[Class[V2]])
      job.setOutputKeyClass(outputKeyType.erasure.asInstanceOf[Class[K3]])
      job.setOutputValueClass(outputValueType.erasure.asInstanceOf[Class[V3]])
    } else {
      job.setOutputKeyClass(mapOutputKeyType.erasure.asInstanceOf[Class[K2]])
      job.setOutputValueClass(mapOutputValueType.erasure.asInstanceOf[Class[V2]])
      job.setNumReduceTasks(0)
    }

    // Apply custom conf modifiers
    confModifiers.foreach { _.apply(job.getConfiguration) }
  }

  def this(mapper: MapperDef[K1, V1, K2, V2], combiner: CombinerDef[K2, V2], reducer: ReducerDef[K2, V2, K3, V3],
           name: String)(implicit k2m: Manifest[K2], v2m: Manifest[V2], k3m: Manifest[K3], v3m: Manifest[V3]) =
    this(mapper.mapperClass, combiner.combinerClass, reducer.reducerClass, name,
      mapper.confModifiers ::: combiner.confModifiers ::: reducer.confModifiers)(k2m, v2m, k3m, v3m)

  def this(mapper: MapperDef[K1, V1, K2, V2], reducer: ReducerDef[K2, V2, K3, V3], name: String)
          (implicit k2m: Manifest[K2], v2m: Manifest[V2], k3m: Manifest[K3], v3m: Manifest[V3]) =
    this(mapper.mapperClass, None, reducer.reducerClass, name,
         mapper.confModifiers ::: reducer.confModifiers)(k2m, v2m, k3m, v3m)
}

class MapOnlyJob[K1, V1, K2, V2] private
    (mapperClass: Class[_ <: Mapper[K1, V1, K2, V2]], name: String, confModifiers: List[ConfModifier] = List())
    (implicit k2m: Manifest[K2], v2m: Manifest[V2])
    extends MapReduceJob[K1, V1, K2, V2, K2, V2](mapperClass, None, None, name, confModifiers)(k2m, v2m, k2m, v2m) {

  def this(mapper: MapperDef[K1, V1, K2, V2], name: String)(implicit k2m: Manifest[K2], v2m: Manifest[V2]) =
    this(mapper.mapperClass, name, mapper.confModifiers)(k2m, v2m)
}
