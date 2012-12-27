package com.twitter.mapreduce.load

import com.tehasdf.sstable.{DataReader, Row}
import com.tehasdf.sstable.input.{InMemorySeekableDataStream, SeekableDataInputStream}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{BytesWritable, MapWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import java.io.InputStream
import com.tehasdf.sstable.Column
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.ArrayWritable
import com.tehasdf.mapreduce.data.WritableColumn
import java.util.ArrayList
import com.tehasdf.mapreduce.data.ColumnState
import com.tehasdf.sstable.Deleted
import com.tehasdf.mapreduce.load.SSTableDataSplit
import java.util.Arrays
import com.tehasdf.sstable.input.SnappyCompressedSeekableDataStream
import com.tehasdf.sstable.SequenceBackedCompressionInfo
import org.apache.hadoop.io.LongWritable
import java.io.ByteArrayInputStream


class SSTableDataRecordReader extends RecordReader[BytesWritable, ArrayWritable] {
  case class DataInput(reader: DataReader, seekable: SeekableDataInputStream, dataInputStream: InputStream)

  protected var reader: Option[DataInput] = None
  private var currentRow: Option[Row] = None

  val Log = LogFactory.getLog(classOf[SSTableDataRecordReader])
  def initialize(genericSplit: InputSplit, context: TaskAttemptContext) {
    try {
      val file = genericSplit.asInstanceOf[SSTableDataSplit]
      val path = file.path
      val dir = path.getParent()
      val fs = path.getFileSystem(context.getConfiguration())
      val dataIs = fs.open(path)
      
      dataIs.seek(file.fileOffset.get)
      val compressedBuf = new Array[Byte](file.fileLength.get.toInt)
      dataIs.readFully(compressedBuf)
      val seekable = new InMemorySeekableDataStream(compressedBuf)
      val offsettedChunks = file.compressionChunks.get().map(_.asInstanceOf[LongWritable].get()).map(_-file.fileOffset.get())
      
      val cinfo = new SequenceBackedCompressionInfo(file.uncompressedLength.get(), offsettedChunks)
      val ds = new SnappyCompressedSeekableDataStream(seekable, cinfo)
      val decompressed = ds.decompressEntireStream
      
      val seekableDecompressed = new InMemorySeekableDataStream(decompressed)
      seekableDecompressed.seek(file.innerOffset.get)
      
      reader = Some(DataInput(new DataReader(seekableDecompressed), seekableDecompressed, dataIs))
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
          case Column(name, data) =>
            arr.add(WritableColumn(ColumnState.Normal, new BytesWritable(name), new BytesWritable(data)))
          case Deleted(name) =>
            arr.add(WritableColumn.deleted(new BytesWritable(name)))
        }
      }
      val rv = new ArrayWritable(classOf[Writable])
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