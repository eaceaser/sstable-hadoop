package com.tehasdf.pig.load

import com.tehasdf.mapreduce.load.SSTableIndexInputFormat

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, RecordReader}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.pig.{Expression, LoadFunc, LoadMetadata, ResourceSchema}
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit
import org.apache.pig.data.{DataType, Tuple, TupleFactory}
import org.apache.pig.impl.logicalLayer.schema.Schema
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema

class SSTableIndexLoader extends LoadFunc with LoadMetadata {
  private var reader: Option[RecordReader[Text, LongWritable]] = None
  private val tupleFactory = TupleFactory.getInstance()
  
  def getInputFormat() = new SSTableIndexInputFormat

  def getNext(): Tuple = {
    reader.map { r =>
      r.nextKeyValue() match {
        case true =>
          val k = r.getCurrentKey().toString()
          tupleFactory.newTuple(k)
        case false => null
      }
    }.getOrElse(null)
  }

  def prepareToRead(r: RecordReader[_, _], split: PigSplit) {
    reader = Some(r.asInstanceOf[RecordReader[Text, LongWritable]])
  }

  def setLocation(loc: String, job: Job) {
    FileInputFormat.setInputPaths(job, loc)
  }
  
  def setPartitionFilter(pf: Expression) { }
  
  def getPartitionKeys(k: String, j: Job) = null
  
  def getStatistics(k: String, j: Job) = null
  
  def getSchema(f: String, j: Job) = {
    new ResourceSchema(new Schema(new FieldSchema("key", DataType.CHARARRAY)))
  }
}