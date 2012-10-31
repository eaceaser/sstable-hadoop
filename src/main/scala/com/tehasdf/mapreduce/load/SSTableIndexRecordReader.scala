package com.tehasdf.mapreduce.load

import com.tehasdf.sstable.IndexReader
import com.tehasdf.sstable.input.{SeekableDataInputStream, SeekableDataInputStreamProxy}

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import java.io.IOException

case class IndexReaderStream(is: FSDataInputStream, seekable: SeekableDataInputStream, reader: IndexReader)

class SSTableIndexRecordReader extends RecordReader[Text, LongWritable] {
  protected var reader: Option[IndexReaderStream] = None
  protected var currentPair: Option[(Text, LongWritable)] = None

  def initialize(genericSplit: InputSplit, context: TaskAttemptContext) {
    val split = genericSplit.asInstanceOf[FileSplit]

    val file = split.getPath;
    val job = context.getConfiguration()
    val fs = file.getFileSystem(job)
    val is = fs.open(split.getPath)
    val seekable = new SeekableDataInputStreamProxy(is) {
      def position = is.getPos()
      def seek(to: Long) = is.seek(to)
      val length = fs.getFileStatus(split.getPath).getLen()
    }
    val indexReader = new IndexReader(seekable)

    reader = Some(IndexReaderStream(is, seekable, indexReader))
  }

  def close() {
    reader.foreach { _.is.close() }
  }

  def getCurrentKey(): Text = currentPair.map(_._1).getOrElse(null)
  def getCurrentValue(): LongWritable = currentPair.map(_._2).getOrElse(null)

  def getProgress(): Float = {
    reader.map { r =>
      r.seekable.position / r.seekable.length.toFloat
    }.getOrElse(0.0f)
  }

  def nextKeyValue(): Boolean = {
    reader.map { r =>
      if (r.reader.hasNext) {
        try {
          val key = r.reader.next()
          currentPair = Some((new Text(key.name), new LongWritable(key.pos)))
          true
        } catch {
          case ex: IOException => false
        }
      } else { false }
    }.getOrElse(false)
  }
}
