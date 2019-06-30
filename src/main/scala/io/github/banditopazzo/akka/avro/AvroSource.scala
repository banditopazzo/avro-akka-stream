package io.github.banditopazzo.akka.avro

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.avro4s.{Decoder, SchemaFor}
import io.github.banditopazzo.avro.AvroReader
import org.apache.hadoop.fs.{FileSystem, Path}

object AvroSource {

  def fromHDFS[T <: Product with Serializable](fs: FileSystem, folderName: String, filter: Path => Boolean = _ => true, recursive: Boolean = false)
                                              (implicit schemaFor: SchemaFor[T], decoder: Decoder[T]): Source[T, NotUsed] = {
    Source.fromIterator(() => AvroReader.readFromHDFSFolder(fs, folderName, filter, recursive))
  }

}
