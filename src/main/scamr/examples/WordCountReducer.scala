package scamr.examples

import org.apache.hadoop.io.Text

import scamr.mapreduce.lib.SumReducer
import org.apache.hadoop.mapreduce.ReduceContext

class WordCountReducer(context: ReduceContext[_,_,_,_]) extends SumReducer[Text](context);