package scamr.mapreduce.reducer

import org.apache.hadoop.mapreduce.Reducer
import scamr.conf.{LambdaConfModifier, ConfModifier}
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}

object ReducerDef {
  type SimpleInjectableClass[K2, V2, K3, V3] = Class[_ <: SimpleReducer[K2, V2, K3, V3] with Injectable]

  implicit def classicReducerToDef[K2, V2, K3, V3](classicReducer: Reducer[K2, V2, K3, V3])
    (implicit k3m: Manifest[K3], v3m: Manifest[V3]) = new ClassicReducerDef(classicReducer)

  implicit def injectableReducerClassToDef[K2, V2, K3, V3](clazz: SimpleInjectableClass[K2, V2, K3, V3])
    (implicit k3m: Manifest[K3], v3m: Manifest[V3], bindingModule: BindingModule) = new SimpleReducerDef(clazz)

  implicit def simpleReducerClassToDef[K2, V2, K3, V3](clazz: Class[_ <: SimpleReducer[K2, V2, K3, V3]])
    (implicit k3m: Manifest[K3], v3m: Manifest[V3]) = new SimpleReducerDef(clazz)

  implicit def lambdaReduceFunctionToDef[K2, V2, K3, V3](lambda: LambdaReducer[K2, V2, K3, V3]#FunctionType)
    (implicit k3m: Manifest[K3], v3m: Manifest[V3]) = new LambdaReducerDef(lambda)
}

trait ReducerDef[K2, V2, K3, V3] {
  val reducerClass: Option[Class[_ <: Reducer[K2, V2, K3, V3]]]
  val confModifiers: List[ConfModifier]
  implicit val k3m: Manifest[K3]
  implicit val v3m: Manifest[V3]
}

class ClassicReducerDef[K2, V2, K3, V3](val classicReducer: Reducer[K2, V2, K3, V3])
                                       (implicit override val k3m: Manifest[K3], override val v3m: Manifest[V3])
                                        extends ReducerDef[K2, V2, K3, V3] {
  override val reducerClass = Some(classicReducer.getClass)
  override val confModifiers = List()
}

class SimpleReducerDef[K2, V2, K3, V3](val simpleReducerClass: Class[_ <: SimpleReducer[K2, V2, K3, V3]])
                                      (implicit override val k3m: Manifest[K3], override val v3m: Manifest[V3])
                                       extends ReducerDef[K2, V2, K3, V3] {
  override val reducerClass = Some(classOf[SimpleReducer.Runner[K2, V2, K3, V3]])
  override val confModifiers =
    List(LambdaConfModifier { conf => SimpleReducer.setSimpleReducerClass(conf, simpleReducerClass) })
}

class InjectableReducerDef[K2, V2, K3, V3]
(val simpleReducerClass: Class[_ <: SimpleReducer[K2, V2, K3, V3] with Injectable])
(implicit override val k3m: Manifest[K3], override val v3m: Manifest[V3], val bindingModule: BindingModule)
extends ReducerDef[K2, V2, K3, V3] {

  override val reducerClass = Some(classOf[SimpleReducer.Runner[K2, V2, K3, V3]])
  override val confModifiers = List(LambdaConfModifier { conf =>
    SimpleReducer.setSimpleReducerClass(conf, simpleReducerClass)
    SimpleReducer.setBindingModuleClass(conf, bindingModule.getClass)
  })
}

class LambdaReducerDef[K2, V2, K3, V3](val lambdaReduceFunction: LambdaReducer[K2, V2, K3, V3]#FunctionType)
                                      (implicit override val k3m: Manifest[K3], override val v3m: Manifest[V3])
                                       extends ReducerDef[K2, V2, K3, V3] {
  override val reducerClass = Some(classOf[LambdaReducer[K2, V2, K3, V3]])
  override val confModifiers =
    List(LambdaConfModifier { conf => LambdaReducer.setLambdaFunction(conf, lambdaReduceFunction) })
}
