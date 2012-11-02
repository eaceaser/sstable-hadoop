package com.tehasdf.mapreduce.util

import org.apache.hadoop.io.Writable

abstract class BinaryConverter[T <: Writable] {
  def convert(bytes: Array[Byte]): T
}
