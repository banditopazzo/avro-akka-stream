package io.github.banditopazzo.akka.avro

import akka.Done
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.sksamuel.avro4s.{Encoder, SchemaFor}
import io.github.banditopazzo.akka.avro.NameGenerator.DefaultNameGenerator
import io.github.banditopazzo.avro.AvroWriter
import org.apache.hadoop.fs.FileSystem

import scala.concurrent.{ExecutionContextExecutor, Future}

object AvroSink {

  def writeManyToHDFS[T <: Product with Serializable](fs: FileSystem, folder: String, parallelism: Int = 4, nameGenerator: NameGenerator = new DefaultNameGenerator)
                                                     (implicit schemaFor: SchemaFor[T], encoder: Encoder[T], system: ActorSystem): Sink[Seq[T], Future[Done]] = {

    implicit val ec: ExecutionContextExecutor = system.dispatcher

    Flow[Seq[T]].mapAsync(parallelism)(elements =>
      Future {
        val name = nameGenerator.generate()
        AvroWriter.writeManyToHadoop(fs, s"$folder/$name", elements)
      }
    ).toMat(Sink.ignore)(Keep.right)

  }

  def writeOneToHDFS[T <: Product with Serializable](fs: FileSystem, folder: String, parallelism: Int = 4, nameGenerator: NameGenerator = new DefaultNameGenerator)
                                                    (implicit schemaFor: SchemaFor[T], encoder: Encoder[T], system: ActorSystem): Sink[T, Future[Done]] = {

    implicit val ec: ExecutionContextExecutor = system.dispatcher

    Flow[T].mapAsync(parallelism)(element =>
      Future {
        val name = nameGenerator.generate()
        AvroWriter.writeOneToHadoop(fs, s"$folder/$name", element)
      }
    ).toMat(Sink.ignore)(Keep.right)

  }

  object Extensions {

    implicit class AvroWriteOneExt[T <: Product with Serializable](val s: Source[T, _]) extends AnyVal {
      def runWithAvroSink(fs: FileSystem, folder: String, parallelism: Int = 4, nameGenerator: NameGenerator = new DefaultNameGenerator)
                         (implicit schemaFor: SchemaFor[T], encoder: Encoder[T], system: ActorSystem, materializer: Materializer): Future[Done] = {
        s.runWith(AvroSink.writeOneToHDFS(fs, folder, parallelism, nameGenerator))
      }
    }

    implicit class AvroWriteManyExt[T <: Product with Serializable](val s: Source[Seq[T], _]) extends AnyVal {
      def runWithAvroSink(fs: FileSystem, folder: String, parallelism: Int = 4, nameGenerator: NameGenerator = new DefaultNameGenerator)
                         (implicit schemaFor: SchemaFor[T], encoder: Encoder[T], system: ActorSystem, materializer: Materializer): Future[Done] = {
        s.runWith(AvroSink.writeManyToHDFS(fs, folder, parallelism, nameGenerator))
      }
    }

  }

}
