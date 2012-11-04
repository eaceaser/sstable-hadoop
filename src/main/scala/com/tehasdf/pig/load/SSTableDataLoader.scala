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

object SSTableDataLoader {
  private[SSTableDataLoader] val Schema = "key:bytearray,columns:{col:(name: bytearray, data: bytearray)}"
}

class SSTableDataLoader extends LoadFunc with LoadMetadata {
  private var reader: Option[RecordReader[BytesWritable, MapWritable]] = None
  private val fact = TupleFactory.getInstance()

  def getInputFormat() = new SSTableDataInputFormat
  def prepareToRead(r: RecordReader[_, _], split: PigSplit) {
    reader = Some(r.asInstanceOf[RecordReader[BytesWritable, MapWritable]])
  }

  def setLocation(loc: String, job: Job) {
    FileInputFormat.setMaxInputSplitSize(job, 512*1024)
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

          val rv = fact.newTuple(2)
          rv.set(0, new DataByteArray(key.getBytes()));

          val keys = cols.keySet()
          val keysIt = keys.iterator()

          val bag = new DefaultDataBag
          while (keysIt.hasNext()) {
            val colNameBytes = keysIt.next()
            val colName = new DataByteArray(colNameBytes.asInstanceOf[BytesWritable].getBytes())
            val colData = new DataByteArray(cols.get(colNameBytes).asInstanceOf[BytesWritable].getBytes())
            val tuple = fact.newTuple(2)
            tuple.set(0, colName)
            tuple.set(1, colData)
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