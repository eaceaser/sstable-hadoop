package com.twitter.mapreduce.load

import com.tehasdf.mapreduce.load.CompressedSSTableSplit
import com.tehasdf.sstable.{DataReader, Row}
import com.tehasdf.sstable.input.{InMemorySeekableDataStream, SeekableDataInputStream}

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{BytesWritable, MapWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

import java.io.InputStream

class SSTableDataRecordReader extends RecordReader[Text, MapWritable] {
  case class DataInput(reader: DataReader, seekable: SeekableDataInputStream, dataInputStream: InputStream)

  protected var reader: Option[DataInput] = None
  private var currentRow: Option[Row] = None

  val Log = LogFactory.getLog(classOf[SSTableDataRecordReader])
  def initialize(genericSplit: InputSplit, context: TaskAttemptContext) {
    val file = genericSplit.asInstanceOf[CompressedSSTableSplit]
    val path = file.path
    val dir = path.getParent()

//    val compressionInfoFile = path.getName().replaceAll("-Data\\.db$", "-CompressionInfo.db")
//    val compressionInfoPath = new Path(dir, compressionInfoFile)
//    println("Reading compression info from file: %s".format(compressionInfoFile))

    val fs = path.getFileSystem(context.getConfiguration())

//    if (!fs.exists(compressionInfoPath)) {
//      throw new IOException("%s does not exist.".format(compressionInfoFile))
//    }

    val dataIs = fs.open(path)
    val compressedBuf = new Array[Byte](file.getLength().toInt)
    dataIs.readFully(file.start, compressedBuf)

//    val seekableDataFile = new SeekableDataInputStreamProxy(dataIs) {
//      def position = dataIs.getPos()
//      def seek(to: Long) = dataIs.seek(to)
//      val length = fs.getFileStatus(path).getLen()
//    }

//    val compressionInfoIs = fs.open(compressionInfoPath)
//    val compressionInfo = new CompressionInfoReader(compressionInfoIs)

//    val compressedIs = new SnappyCompressedSeekableDataStream(seekableDataFile, compressionInfo)
    val seekable = InMemorySeekableDataStream.fromSnappyCompressedData(compressedBuf, file.compressionOffsets)
    seekable.seek(file.firstKeyPosition)
    reader = Some(DataInput(new DataReader(seekable), seekable, dataIs))
  }

  def close() = {
    reader.foreach { r =>
      r.dataInputStream.close()
    }
  }

  def getProgress() = { reader.map { r => r.seekable.position / r.seekable.length.toFloat }.getOrElse(0.0f) }

  def getCurrentValue() = {
    currentRow.map { row =>
      val rv = new MapWritable
      row.columns.foreach { column =>
        rv.put(new BytesWritable(column.name), new BytesWritable(column.data))
      }
      rv
    }.getOrElse(null)
  }

  def getCurrentKey() = {
    currentRow.map(r => new Text(r.key)).getOrElse(null)
  }

  def nextKeyValue() = {
    reader.map { data =>
      if (data.reader.hasNext) {
        currentRow = Some(data.reader.next())
        true
      } else {
        currentRow = None
        false
      }
    }.getOrElse(false)
  }
}