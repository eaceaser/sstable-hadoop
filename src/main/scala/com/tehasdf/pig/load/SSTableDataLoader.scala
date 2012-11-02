package com.tehasdf.pig.load

import com.tehasdf.mapreduce.load.SSTableDataInputFormat
import org.apache.hadoop.io.{BytesWritable, MapWritable, Text}
import org.apache.hadoop.mapreduce.{Job, RecordReader}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.pig.{Expression, LoadFunc, LoadMetadata, ResourceSchema}
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit
import org.apache.pig.data.{DataByteArray, Tuple, TupleFactory}
import org.apache.pig.impl.util.Utils
import com.tehasdf.mapreduce.util.BinaryConverter
import org.apache.hadoop.io.Writable

object SSTableDataLoader {
  private[SSTableDataLoader] val Schema = "key:chararray,columns:[]"
}

abstract class SSTableDataLoader[Key <: Writable : BinaryConverter, ColumnName <: Writable : BinaryConverter, ColumnValue <: Writable : BinaryConverter] extends LoadFunc with LoadMetadata {
  private var reader: Option[RecordReader[Text, MapWritable]] = None
  private val fact = TupleFactory.getInstance()

  def getInputFormat() = new SSTableDataInputFormat[Key, ColumnName, ColumnValue]
  def prepareToRead(r: RecordReader[_, _], split: PigSplit) {
    reader = Some(r.asInstanceOf[RecordReader[Text, MapWritable]])
  }

  def setLocation(loc: String, job: Job) {
    FileInputFormat.setMaxInputSplitSize(job, 512*1024)
    FileInputFormat.setInputPaths(job, loc)
  }

  def setPartitionFilter(pf: Expression) {}
  def getPartitionKeys(k: String, j: Job) = null
  def getStatistics(k: String, j: Job) = null

  def getSchema(f: String, j: Job) = {
/*    val bagTuple = Array[Schema.FieldSchema](
      new Schema.FieldSchema("name", DataType.BYTEARRAY),
      new Schema.FieldSchema("value", DataType.BYTEARRAY))

    val fields = Array[Schema.FieldSchema](
      new Schema.FieldSchema("key", DataType.CHARARRAY),
      new Schema.FieldSchema("columns", new Schema(new Schema.FieldSchema("t", new Schema(Arrays.asList(bagTuple: _*)), DataType.TUPLE)), DataType.BAG)
    )

    new ResourceSchema(new Schema(Arrays.asList(fields: _*))) */
    new ResourceSchema(Utils.getSchemaFromString(SSTableDataLoader.Schema))
  }

  def getNext(): Tuple = {
    reader.map { r =>
      r.nextKeyValue() match {
        case true =>
          val key = r.getCurrentKey()
          val cols = r.getCurrentValue()

          val rv = fact.newTuple()
          rv.append(key.toString())

          val keys = cols.keySet()
          val keysIt = keys.iterator()
          val map = new java.util.HashMap[DataByteArray, DataByteArray]()
          while (keysIt.hasNext()) {
            val colNameStr = keysIt.next()
            val colName = new DataByteArray(colNameStr.asInstanceOf[BytesWritable].getBytes())
            val colData = new DataByteArray(cols.get(colNameStr).asInstanceOf[BytesWritable].getBytes())
            map.put(colName, colData)
          }
          rv.append(map)
          rv
        case false =>
          null
      }
    }.getOrElse(null)
  }

}