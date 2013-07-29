package scamr.conf

object ConfigureSpeculativeExecution {
  def apply(mapper: Boolean, reducer: Boolean): ConfModifier = LambdaConfModifier { conf =>
    conf.setBoolean("mapred.map.tasks.speculative.execution", mapper)
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", reducer)
  }
}
