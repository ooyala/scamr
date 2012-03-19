package scamr.conf

object SetEnumParam {
  def apply[T <: Enum[T]](name: String, value: T) = LambdaConfModifier { _.setEnum(name, value) }
}