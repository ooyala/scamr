package scamr.examples

import org.apache.hadoop.io.Text

import scamr.mapreduce.lib.SumReducer

class WordCountReducer(context: WordCountReducer#ContextType) extends SumReducer[Text](context);