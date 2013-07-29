package scamr.conf

object SetEnumParam {
  def apply[T <: Enum[T]](name: String, value: T): ConfModifier = LambdaConfModifier { _.setEnum(name, value) }
}
