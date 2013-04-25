package scamr.mapreduce.mapper

import org.apache.hadoop.mapreduce.Mapper
import scamr.conf.{LambdaConfModifier, ConfModifier}
import com.escalatesoft.subcut.inject.{BindingModule, Injectable}

object MapperDef {
  implicit def classicMapperToDef[K1, V1, K2, V2](classicMapper: Mapper[K1, V1, K2, V2])
    (implicit k2m: Manifest[K2], v2m: Manifest[V2]) = new ClassicMapperDef(classicMapper)

  implicit def injectableMapperClassToDef[K1, V1, K2, V2](clazz: Class[_ <: SimpleMapper[K1, V1, K2, V2] with Injectable])
    (implicit k2m: Manifest[K2], v2m: Manifest[V2], bindingModule: BindingModule) = new InjectableMapperDef(clazz)

  implicit def simpleMapperClassToDef[K1, V1, K2, V2](clazz: Class[_ <: SimpleMapper[K1, V1, K2, V2]])
    (implicit k2m: Manifest[K2], v2m: Manifest[V2]) = new SimpleMapperDef(clazz)

  implicit def lambdaMapFunctionToDef[K1, V1, K2, V2](lambda: LambdaMapper[K1, V1, K2, V2]#FunctionType)
    (implicit k2m: Manifest[K2], v2m: Manifest[V2]) = new LambdaMapperDef(lambda)
}

trait MapperDef[K1, V1, K2, V2] {
  val mapperClass: Class[_ <: Mapper[K1, V1, K2, V2]]
  val confModifiers: List[ConfModifier]
  implicit val k2m: Manifest[K2]
  implicit val v2m: Manifest[V2]
}

class ClassicMapperDef[K1, V1, K2, V2](val classicMapper: Mapper[K1, V1, K2, V2])
                                      (implicit override val k2m: Manifest[K2], override val v2m: Manifest[V2])
                                       extends MapperDef[K1, V1, K2, V2] {
  override val mapperClass = classicMapper.getClass
  override val confModifiers = List()
}

class SimpleMapperDef[K1, V1, K2, V2](val simpleMapperClass: Class[_ <: SimpleMapper[K1, V1, K2, V2]])
                                     (implicit override val k2m: Manifest[K2], override val v2m: Manifest[V2])
                                      extends MapperDef[K1, V1, K2, V2] {
  override val mapperClass = classOf[SimpleMapper.Runner[K1, V1, K2, V2]]
  override val confModifiers =
    List(LambdaConfModifier { conf => SimpleMapper.setSimpleMapperClass(conf, simpleMapperClass) })
}

class InjectableMapperDef[K1, V1, K2, V2]
(val simpleMapperClass: Class[_ <: SimpleMapper[K1, V1, K2, V2] with Injectable])
(implicit override val k2m: Manifest[K2], override val v2m: Manifest[V2], val bindingModule: BindingModule)
extends MapperDef[K1, V1, K2, V2] {

  override val mapperClass = classOf[SimpleMapper.Runner[K1, V1, K2, V2]]
  override val confModifiers = List(LambdaConfModifier { conf =>
    SimpleMapper.setSimpleMapperClass(conf, simpleMapperClass)
    SimpleMapper.setBindingModuleClass(conf, bindingModule.getClass)
  })
}

class LambdaMapperDef[K1, V1, K2, V2](val lambdaMapFunction: LambdaMapper[K1, V1, K2, V2]#FunctionType)
                                     (implicit override val k2m: Manifest[K2], override val v2m: Manifest[V2])
                                      extends MapperDef[K1, V1, K2, V2] {
  override val mapperClass = classOf[LambdaMapper[K1, V1, K2, V2]]
  override val confModifiers =
    List(LambdaConfModifier { conf => LambdaMapper.setLambdaFunction(conf, lambdaMapFunction) })
}
