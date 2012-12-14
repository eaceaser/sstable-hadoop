package com.tehasdf.mapreduce.load

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import com.tehasdf.mapreduce.util.FSSeekableDataInputStream
import com.tehasdf.sstable.IndexReader
import com.tehasdf.sstable.input.{BoundedSeekableDataInputStreamProxy, SeekableDataInputStream}

case class IndexReaderStream(is: FSDataInputStream, seekable: SeekableDataInputStream, reader: IndexReader)

class SSTableIndexRecordReader extends RecordReader[Text, LongWritable] {
  private val Log = LogFactory.getLog(this.getClass())
  protected var reader: Option[IndexReaderStream] = None
  protected var currentPair: Option[(Text, LongWritable)] = None

  def initialize(genericSplit: InputSplit, context: TaskAttemptContext) {
    val split = genericSplit.asInstanceOf[FileSplit]

    val file = split.getPath
    val job = context.getConfiguration()
    val fs = file.getFileSystem(job)
    val is = fs.open(file)
    val status = fs.getFileStatus(file)
    val seekable = new FSSeekableDataInputStream(is, status)
    val bounded = new BoundedSeekableDataInputStreamProxy(seekable, split.getStart(), split.getLength())
    val indexReader = new IndexReader(bounded)
    reader = Some(IndexReaderStream(is, bounded, indexReader))
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
        val key = r.reader.next()
        currentPair = Some((new Text(key.name), new LongWritable(key.pos)))
        true
      } else { 
        false 
      }
    }.getOrElse(false)
  }
}
