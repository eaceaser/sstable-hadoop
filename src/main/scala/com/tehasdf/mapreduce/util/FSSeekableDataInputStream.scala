package com.tehasdf.mapreduce.util

import com.tehasdf.sstable.input.SeekableDataInputStreamProxy

import org.apache.hadoop.fs.{FSDataInputStream, FileStatus}

class FSSeekableDataInputStream(is: FSDataInputStream, fs: FileStatus) extends SeekableDataInputStreamProxy(is) {
  def position = is.getPos()
  def seek(to: Long) = is.seek(to)
  val length = fs.getLen()
}