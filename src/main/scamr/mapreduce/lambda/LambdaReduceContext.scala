package scamr.mapreduce.lambda

import org.apache.hadoop.mapreduce.ReduceContext

class LambdaReduceContext(context: ReduceContext[_, _, _, _]) extends BaseLambdaContext(context);