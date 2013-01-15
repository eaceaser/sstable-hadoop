package com.twitter.mapreduce.load

import com.tehasdf.sstable.{DataReader, Row}
import scala.collection.mutable
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.fs.Path
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.hadoop.conf.Configuration
import java.io.IOException


class SSTableDataRecordReader extends RecordReader[BytesWritable, ArrayWritable] {
  case class DataInput(reader: DataReader, seekable: SeekableDataInputStream, dataInputStream: InputStream)

  case class SplitIndex(filename: String, byteStart: Long, byteLength: Long)
  case class SplitRecord(innerOffset: Long, innerLength: Long, uncompressedLength: Long, compressionChunks: Seq[Long])
  protected var reader: Option[DataInput] = None
  private var currentRow: Option[Row] = None

  val Log = LogFactory.getLog(classOf[SSTableDataRecordReader])
  
  private def readSplitData(root: Path, idx: SplitIndex, conf: Configuration) = {
    val fs = root.getFileSystem(conf)
    val pattern = new Path(root, "part-*")
    var rv: Option[SplitRecord] = None
    fs.globStatus(pattern).foreach { status => 
      val is = fs.open(status.getPath())
      val buf = new BufferedReader(new InputStreamReader(is))
      var line = buf.readLine()
      while (line != null) {
        line.split('\t').toList match {
          case filename :: index:: byteStart :: byteLength :: innerOffset :: innerLength :: uncompressedLength :: compressionChunks :: Nil =>
            val rec = SplitRecord(innerOffset.toLong, innerLength.toLong, uncompressedLength.toLong, compressionChunks.split(",").map(_.toLong))
            val compIndex = SplitIndex(filename, byteStart.toLong, byteLength.toLong)
            Log.info("Comparing %s to %s".format(compIndex, idx))
            if (idx == compIndex) {
              rv = Some(rec)
            }
          case _ =>
            throw new IOException("omg: %s".format(line))
        }
        
        line = buf.readLine()
      }
      is.close()
    }
    rv.get
  }
  
  def initialize(genericSplit: InputSplit, context: TaskAttemptContext) {
    try {
      val file = genericSplit.asInstanceOf[FileSplit]
      val conf = context.getConfiguration()
      val path = file.getPath()
      val dir = path.getParent()
      val fs = path.getFileSystem(conf)
      val dataIs = fs.open(path)
      val keyRecord = SplitIndex(file.getPath().getName.stripSuffix("-Data.db"), file.getStart(), file.getLength())
      val splitRecord = readSplitData(new Path(conf.get("sstable.split.dir")), keyRecord, conf)
      
      dataIs.seek(file.getStart())
      val compressedBuf = new Array[Byte](file.getLength().toInt)
      Log.info("Reading file: %s (%d)".format(path.getName(), file.getLength().toLong))
      dataIs.readFully(compressedBuf)
      val seekable = new InMemorySeekableDataStream(compressedBuf)
      
      val offsettedChunks = splitRecord.compressionChunks.map(_-file.getStart())
      
      Log.info("Attempting to decompress chunk of size: %d with %d compression chunks".format(seekable.length, offsettedChunks.size))
      val cinfo = new SequenceBackedCompressionInfo(splitRecord.innerLength, offsettedChunks)
      val ds = new SnappyCompressedSeekableDataStream(seekable, cinfo)
      val decompressed = ds.decompressEntireStream
      
      val seekableDecompressed = new InMemorySeekableDataStream(decompressed)
      seekableDecompressed.seek(splitRecord.innerOffset)
      
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
          case Column(name, data, timestamp) =>
            arr.add(WritableColumn(ColumnState.Normal, new BytesWritable(name), new BytesWritable(data), new LongWritable(timestamp)))
          case Deleted(name, timestamp) =>
            arr.add(WritableColumn.deleted(new BytesWritable(name), new LongWritable(timestamp)))
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