package com.twitter.mapreduce.load

import com.tehasdf.sstable.DataReader
import com.tehasdf.sstable.SequenceBackedCompressionInfo
import com.tehasdf.sstable.input.{SnappyCompressedSeekableDataStream, InMemorySeekableDataStream}
import java.io.{BufferedReader, IOException, InputStreamReader}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext}

class SplitSSTableDataRecordReader extends SSTableDataRecordReader {
  case class SplitIndex(filename: String, byteStart: Long, byteLength: Long)
  case class SplitRecord(innerOffset: Long, innerLength: Long, uncompressedLength: Long, compressionChunks: Seq[Long])

  private val Log = LogFactory.getLog(classOf[SplitSSTableDataRecordReader])
  
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
            Log.debug("Comparing %s to %s".format(compIndex, idx))
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
  
  override def initialize(genericSplit: InputSplit, context: TaskAttemptContext) {
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
      
      Log.info("Attempting to decompress chunk of size: %d with %d compression chunks and an uncompressed length of %d/%d".format(seekable.length, offsettedChunks.size, splitRecord.innerLength, splitRecord.uncompressedLength))
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
}
