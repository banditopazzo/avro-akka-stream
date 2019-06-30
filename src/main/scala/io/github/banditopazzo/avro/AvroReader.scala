package io.github.banditopazzo.avro

import java.io.InputStream

import com.sksamuel.avro4s.{AvroInputStream, Decoder, SchemaFor}
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

object AvroReader {

  def readFromInputStream[T <: Product with Serializable](inputStream: InputStream)
                                                         (implicit schemaFor: SchemaFor[T], decoder: Decoder[T]): Iterator[T] = {
    val schema = schemaFor.schema

    val is = AvroInputStream.data[T].from(inputStream).build(schema)
    val elements = is.iterator
    is.close()
    elements
  }

  def readFromHDFSFile[T <: Product with Serializable](fs: FileSystem, fileName: String)
                                                      (implicit schemaFor: SchemaFor[T], decoder: Decoder[T]): Iterator[T] = {
    val inputStream = fs.open(new Path(fileName))
    readFromInputStream(inputStream)
  }

  def readFromHDFSFolder[T <: Product with Serializable](fs: FileSystem, folderName: String, filter: Path => Boolean = _ => true, recursive: Boolean = false)
                                                        (implicit schemaFor: SchemaFor[T], decoder: Decoder[T]): Iterator[T] = {

    fs.listFiles(new Path(folderName), recursive)
      .map(_.getPath)
      .filter(filter)
      .map(fs.open)
      .flatMap(readFromInputStream(_))
  }

  implicit class RemoteIteratorWrapper(ri: RemoteIterator[LocatedFileStatus]) extends Iterator[LocatedFileStatus] {
    override def hasNext: Boolean = ri.hasNext

    override def next(): LocatedFileStatus = ri.next()
  }

}
