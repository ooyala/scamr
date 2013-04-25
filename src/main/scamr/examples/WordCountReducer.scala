package scamr.examples

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.ReduceContext
import scamr.mapreduce.lib.SumReducer


class WordCountReducer(context: ReduceContext[_,_,_,_]) extends SumReducer[Text](context);