package scamr.conf

object ConfigureSpeculativeExecution {
  def apply(mapper: Boolean, reducer: Boolean): ConfModifier = LambdaConfModifier { conf =>
    conf.setBoolean(HadoopVersionSpecific.ConfKeys.SpeculativeMappers, mapper)
    conf.setBoolean(HadoopVersionSpecific.ConfKeys.SpeculativeReducers, reducer)
  }
}
