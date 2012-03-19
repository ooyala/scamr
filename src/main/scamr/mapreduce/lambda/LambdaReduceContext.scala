package scamr.mapreduce.lambda

import org.apache.hadoop.mapreduce.ReduceContext

class LambdaReduceContext[K1, V1, K2, V2](context: ReduceContext[K1, V1, K2, V2])
  extends BaseLambdaContext[K1, V1, K2, V2](context);