package com.tehasdf.mapreduce.load

import java.io.IOException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.tehasdf.sstable.IndexReader
import com.tehasdf.sstable.input.SeekableDataInputStream
import com.tehasdf.sstable.input.SeekableDataInputStreamProxy

object SSTableIndexRecordReader {
  val Log = LoggerFactory.getLogger(classOf[SSTableIndexRecordReader])
}

case class IndexReaderStream(is: FSDataInputStream, seekable: SeekableDataInputStream, reader: IndexReader)

class SSTableIndexRecordReader extends RecordReader[Text, LongWritable] {
  protected var fileStart: Long = 0L
  protected var reader: Option[IndexReaderStream] = None 
  protected var currentPair: Option[(Text, LongWritable)] = None
  
  def initialize(genericSplit: InputSplit, context: TaskAttemptContext) {
    val split = genericSplit.asInstanceOf[FileSplit]
    fileStart = split.getStart();
    val end = fileStart + split.getLength;
    
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
        val key = r.reader.next()
        currentPair = Some((new Text(key.name), new LongWritable(key.pos)))
        true
      } else { false }
    }.getOrElse(false)
  }
}
