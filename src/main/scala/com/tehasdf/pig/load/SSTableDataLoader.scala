package com.tehasdf.pig.load

import com.tehasdf.mapreduce.load.SSTableDataInputFormat
import org.apache.hadoop.io.{BytesWritable, MapWritable, Text}
import org.apache.hadoop.mapreduce.{Job, RecordReader}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.pig.{Expression, LoadFunc, LoadMetadata, ResourceSchema}
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit
import org.apache.pig.data.{DataByteArray, Tuple, TupleFactory}
import org.apache.pig.impl.util.Utils
import org.apache.pig.data.DefaultDataBag
import org.apache.hadoop.io.ArrayWritable
import com.tehasdf.mapreduce.data.WritableColumn

object SSTableDataLoader {
  private[SSTableDataLoader] val Schema = "key:bytearray,columns:{col:(name: bytearray, state: chararray, data: bytearray)}"
}

class SSTableDataLoader extends LoadFunc with LoadMetadata {
  private var reader: Option[RecordReader[BytesWritable, ArrayWritable]] = None
  private val fact = TupleFactory.getInstance()

  def getInputFormat() = new SSTableDataInputFormat
  def prepareToRead(r: RecordReader[_, _], split: PigSplit) {
    reader = Some(r.asInstanceOf[RecordReader[BytesWritable, ArrayWritable]])
  }

  def setLocation(loc: String, job: Job) {
    FileInputFormat.setMaxInputSplitSize(job, 16*1024*1024)
    FileInputFormat.setInputPaths(job, loc)
  }

  def setPartitionFilter(pf: Expression) {}
  def getPartitionKeys(k: String, j: Job) = null
  def getStatistics(k: String, j: Job) = null

  def getSchema(f: String, j: Job) = { new ResourceSchema(Utils.getSchemaFromString(SSTableDataLoader.Schema)) }

  def getNext(): Tuple = {
    reader.map { r =>
      r.nextKeyValue() match {
        case true =>
          val key = r.getCurrentKey()
          val cols = r.getCurrentValue()
          val colsArray = cols.get()

          val rv = fact.newTuple(2)
          rv.set(0, new DataByteArray(key.getBytes()));

          val bag = new DefaultDataBag
          for (column <- colsArray) {
            val casted = column.asInstanceOf[WritableColumn]
            val tuple = fact.newTuple(3)
            tuple.set(0, new DataByteArray(casted.name.getBytes()))
            tuple.set(1, casted.state.toString())
            casted.state match {
              case WritableColumn.State.NORMAL => tuple.set(2, new DataByteArray(casted.data.getBytes()))
              case WritableColumn.State.DELETED =>
              case WritableColumn.State.EXPIRING =>
            }

            bag.add(tuple)
          }

          rv.set(1, bag)
          rv
        case false =>
          null
      }
    }.getOrElse(null)
  }
}