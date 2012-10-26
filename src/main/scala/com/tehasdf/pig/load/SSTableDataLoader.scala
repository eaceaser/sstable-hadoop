package com.tehasdf.pig.load

import org.apache.pig.LoadFunc
import org.apache.pig.LoadMetadata
import com.tehasdf.mapreduce.load.SSTableDataInputFormat
import org.apache.pig.data.Tuple
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.pig.Expression
import org.apache.pig.ResourceSchema
import org.apache.pig.impl.logicalLayer.schema.Schema
import org.apache.pig.data.DataBag
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema
import org.apache.pig.data.DataType
import java.util.Arrays
import org.apache.pig.data.DefaultTuple
import org.apache.pig.data.DefaultDataBag
import java.util.ArrayList
import org.apache.hadoop.io.BytesWritable
import org.apache.pig.data.TupleFactory
import org.apache.pig.data.DataByteArray

class SSTableDataLoader extends LoadFunc with LoadMetadata {
  private var reader: Option[RecordReader[Text, MapWritable]] = None
  private val fact = TupleFactory.getInstance()

  def getInputFormat() = new SSTableDataInputFormat
  def prepareToRead(r: RecordReader[_, _], split: PigSplit) {
    reader = Some(r.asInstanceOf[RecordReader[Text, MapWritable]])
  }

  def setLocation(loc: String, job: Job) {
    FileInputFormat.setInputPaths(job, loc)
  }

  def setPartitionFilter(pf: Expression) {}
  def getPartitionKeys(k: String, j: Job) = null
  def getStatistics(k: String, j: Job) = null

  def getSchema(f: String, j: Job) = {
    val bagTuple = Array[Schema.FieldSchema](
      new Schema.FieldSchema("name", DataType.BYTEARRAY),
      new Schema.FieldSchema("value", DataType.BYTEARRAY))

    val fields = Array[Schema.FieldSchema](
      new Schema.FieldSchema("key", DataType.CHARARRAY),
      new Schema.FieldSchema("columns", new Schema(new Schema.FieldSchema("t", new Schema(Arrays.asList(bagTuple: _*)), DataType.TUPLE)), DataType.BAG)
    )

    new ResourceSchema(new Schema(Arrays.asList(fields: _*)))
  }

  def getNext(): Tuple = {
    reader.map { r =>
      r.nextKeyValue() match {
        case true =>
          val key = r.getCurrentKey()
          val cols = r.getCurrentValue()

          val rv = fact.newTuple()
          rv.append(key)

          val keys = cols.keySet()
          val keysIt = keys.iterator()
          val bagTuples = new ArrayList[Tuple]()
          while (keysIt.hasNext()) {
            val tuple = fact.newTuple()
            val colName = keysIt.next()
            tuple.append(new DataByteArray(colName.asInstanceOf[BytesWritable].getBytes()))
            val colData = new DataByteArray(cols.get(colName).asInstanceOf[BytesWritable].getBytes())
            tuple.append(colData)
            bagTuples.add(tuple)
          }
          val colBag = new DefaultDataBag(bagTuples)
          rv.append(colBag)
          rv
        case false =>
          null
      }
    }.getOrElse(null)
  }

}