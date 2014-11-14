package scamr.conf

import org.apache.hadoop.mapreduce.MRJobConfig

object ConfigureSpeculativeExecution {
  def apply(mapper: Boolean, reducer: Boolean): ConfModifier = LambdaConfModifier { conf =>
    conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, mapper)
    conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, reducer)
  }
}
