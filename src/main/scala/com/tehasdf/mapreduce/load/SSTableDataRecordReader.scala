package com.twitter.mapreduce.load

import com.tehasdf.mapreduce.data.WritableColumn
import com.tehasdf.mapreduce.util.FSSeekableDataInputStream
import com.tehasdf.sstable.input._
import com.tehasdf.sstable._
import java.io.InputStream
import java.util.ArrayList
import org.apache.hadoop.io.{ArrayWritable, BytesWritable, LongWritable, Writable}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.fs.Path
import com.tehasdf.sstable.Row
import com.tehasdf.sstable.Deleted
import scala.Some
import com.tehasdf.sstable.Column
import com.tehasdf.mapreduce.mapred.ColumnArrayWritable

object SSTableDataRecordReader {
  def pathToCompressionInfo(path: String) = path.replaceAll("-Data\\.db$", "-CompressionInfo.db")
}

class SSTableDataRecordReader extends RecordReader[BytesWritable, ColumnArrayWritable] {
  case class DataInput(reader: DataReader, seekable: SeekableDataInputStream, dataInputStream: InputStream)
  protected var reader: Option[DataInput] = None
  protected var currentRow: Option[Row] = None

  def initialize(genericSplit: InputSplit, context: TaskAttemptContext) {
    try {
      val file = genericSplit.asInstanceOf[FileSplit]
      val conf = context.getConfiguration
      val path = file.getPath
      val fs = path.getFileSystem(conf)
      val dataIs = fs.open(path)
      val fileStat = fs.getFileStatus(path)

      val ds = if (conf.getBoolean("sstable.compressed", false)) {
        val compressionInfo = new Path(SSTableDataRecordReader.pathToCompressionInfo(path.toString))
        val compressionInfoIs = fs.open(compressionInfo)
        val compressionInfoReader = new CompressionInfoReader(compressionInfoIs)
        val is = new FSSeekableDataInputStream(dataIs, fileStat)
        new SnappyCompressedSeekableDataStream(is, compressionInfoReader)
      } else {
        new FSSeekableDataInputStream(dataIs, fileStat)
      }

      reader = Some(DataInput(new DataReader(ds), ds, dataIs))
    } catch {
      case ex: Throwable => ex.printStackTrace(); throw ex;
    }
  }

  def close() = {
    reader.foreach { r =>
      r.dataInputStream.close()
    }
  }

  def getProgress() = { reader.map { r => r.seekable.position / r.seekable.length.toFloat }.getOrElse(0.0f) }

  def getCurrentValue() = try {
    currentRow.map { row =>
      val arr = new ArrayList[Writable]()
      row.columns.foreach { column =>
        column match {
          case Column(name, data, timestamp) =>
            arr.add(new WritableColumn(WritableColumn.State.NORMAL, new BytesWritable(name), new BytesWritable(data), new LongWritable(timestamp)))
          case Deleted(name, timestamp) =>
            arr.add(new WritableColumn(WritableColumn.State.DELETED, new BytesWritable(name), null, new LongWritable(timestamp)))
          case Expiring(name, data, ttl, expiration, timestamp) =>
            arr.add(new WritableColumn(WritableColumn.State.EXPIRING, new BytesWritable(name), new BytesWritable(data), new LongWritable(ttl), new LongWritable(expiration), new LongWritable(timestamp)))
        }
      }
      val rv = new ColumnArrayWritable
      val writableArray = arr.toArray(new Array[Writable](0))
      rv.set(writableArray)
      rv
    }.getOrElse(null)
  } catch {
    case ex: Throwable => ex.printStackTrace(); throw ex
  }

  def getCurrentKey() = currentRow.map(r => new BytesWritable(r.key)).getOrElse(null)

  def nextKeyValue() = try {
    reader.map { data =>
      if (data.reader.hasNext) {
        currentRow = Some(data.reader.next())
        true
      } else {
        currentRow = None
        false
      }
    }.getOrElse(false)
  } catch {
case ex: Throwable => ex.printStackTrace(); false
}
}