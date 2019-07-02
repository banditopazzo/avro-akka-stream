package io.github.banditopazzo.avro

import java.io.{File, FileOutputStream, OutputStream}

import com.sksamuel.avro4s.{AvroOutputStream, Encoder, SchemaFor}
import org.apache.hadoop.fs.{FileSystem, Path}

object AvroWriter {

  def writeOneToHadoop[T <: Product with Serializable](fs: FileSystem, fileName: String, element: T)
                                                      (implicit schemaFor: SchemaFor[T], encoder: Encoder[T]): Unit = {
    val outputStream = fs.create(new Path(fileName))
    writeOneToOutputStream(element, outputStream)
  }

  def writeManyToHadoop[T <: Product with Serializable](fs: FileSystem, fileName: String, elements: Seq[T])
                                                       (implicit schemaFor: SchemaFor[T], encoder: Encoder[T]): Unit = {
    val outputStream = fs.create(new Path(fileName))
    writeManyToOutputStream(elements, outputStream)
  }

  def writeOneToLocal[T <: Product with Serializable](fileName: String, element: T)
                                                     (implicit schemaFor: SchemaFor[T], encoder: Encoder[T]): Unit = {
    val outputStream = new FileOutputStream(new File(fileName))
    writeOneToOutputStream(element, outputStream)
  }

  def writeManyToLocal[T <: Product with Serializable](fileName: String, elements: Seq[T])
                                                      (implicit schemaFor: SchemaFor[T], encoder: Encoder[T]): Unit = {
    val outputStream = new FileOutputStream(new File(fileName))
    writeManyToOutputStream(elements, outputStream)
  }

  def writeOneToOutputStream[T <: Product with Serializable](element: T, outputStream: OutputStream)
                                                            (implicit schemaFor: SchemaFor[T], encoder: Encoder[T]): Unit = {
    writeManyToOutputStream(Seq(element), outputStream)
  }

  def writeManyToOutputStream[T <: Product with Serializable](elements: Seq[T], outputStream: OutputStream)
                                                             (implicit schemaFor: SchemaFor[T], encoder: Encoder[T]): Unit = {
    val schema = schemaFor.schema
    val os = AvroOutputStream.data[T].to(outputStream).build(schema)
    os.write(elements)
    os.flush()
    os.close()
  }

}
