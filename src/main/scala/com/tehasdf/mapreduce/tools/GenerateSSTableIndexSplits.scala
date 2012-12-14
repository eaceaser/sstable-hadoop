package com.tehasdf.mapreduce.tools

import java.io.IOException
import scala.collection.JavaConversions.{asScalaBuffer, bufferAsJavaList}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, Job, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat
import com.tehasdf.mapreduce.util.FSSeekableDataInputStream
import com.tehasdf.sstable.{IndexPosition, IndexSummaryReader}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.NullWritable

object GenerateSSTableIndexSplits {
  case class Chunk(start: Long, len: Long)
  def main(rawArgs: Array[String]) {
    val conf = new Configuration
    val args = new GenericOptionsParser(conf, rawArgs).getRemainingArgs()
    val inputFile = new Path(args(0))
    val fs = inputFile.getFileSystem(conf)
    val is = fs.open(inputFile)
    val status = fs.getFileStatus(inputFile)
    val dis = new FSSeekableDataInputStream(is, status)
    val indexFile = new Path(args(1))
    val indexStatus = fs.getFileStatus(indexFile)
    val reader = new IndexSummaryReader(dis)
    
    val outFile = new Path(args(2))
    val os = fs.create(outFile)
    var prevPos = 0L
    
    println("computing splits for %s from %s".format(indexFile.toString, inputFile.toString))
    reader.foreach { position =>
      val len = position.location - prevPos
      val locs = fs.getFileBlockLocations(indexStatus, prevPos, len)
      val chunk = new FileSplit(indexFile, prevPos, len, locs.map { loc => loc.toString() })
      chunk.write(os)
      
      prevPos = position.location
    }
    
    os.close()
    is.close()
    fs.close()
    println("done.")
  }
}