# avro-akka-stream

Simple Akka Streams connector for handle Avro files.

The schema/class generation and serializing/deserializing is made thought [avro4s](https://github.com/sksamuel/avro4s)

With the actual implementation, it's possible to read/write from/to:
* local filesystem
* HDFS
* InputStream/OutputStream

## Akka Streams connectors

### Source

```scala
import akka.stream.scaladsl.Source
import akka.NotUsed
import akka.actor.ActorSystem
import scala.concurrent.duration._
import org.apache.hadoop.fs.FileSystem
import io.github.banditopazzo.akka.avro.AvroSource

// Define a case class
case class Person(name: String, age: Int)

val fs: FileSystem = ??? // Get Hadoop file system

// Read only top level files from folder having ".avro" extension
val source: Source[Person, NotUsed] =
  AvroSource.fromHDFS[Person](
    fs,
    "/path/to/folder",
    filter = _.getName.endsWith(".avro"),
    recursive = false
  )
```

### Sink

```scala
import akka.stream.scaladsl.Source
import akka.stream.Materializer
import akka.NotUsed
import akka.actor.ActorSystem
import scala.concurrent.duration._
import org.apache.hadoop.fs.FileSystem
import io.github.banditopazzo.akka.avro.AvroSink

// Define a case class
case class Person(name: String, age: Int)

implicit val system: ActorSystem = ??? // Get your actor system
implicit val materializer: Materializer = ???
val fs: FileSystem = ??? // Get Hadoop file system
val source: Source[Person, NotUsed] = ??? // Get the example source

// Aggregate elements and write them in a HDFS folder
source
.groupedWithin(3000, 3.minutes)
.runWith(AvroSink.writeManyToHDFS(fs, "/path/to/folder"))
```

## Basic API

The Akka Streams connectors are built on basic functions. The usage is also simple and there are few input/output options.
Basically you can extend with every source/destination supporting InputStream/OutputStream

See the example of reading/writing to a local file:

### Read

```scala
import io.github.banditopazzo.avro.AvroReader

// Define a case class
case class Person(name: String, age: Int)

// Read data 
val data: Iterator[Person] = AvroReader.readFromFileLocal[Person]("/path/to/file")

```

### Write

```scala
import io.github.banditopazzo.avro.AvroWriter

// Define a case class
case class Person(name: String, age: Int)

// Create some elements
val p1 = Person("Bob", 20)
val p2 = Person("Alice", 20)
val list = List(p1,p2)

// Write single element
AvroWriter.writeOneToLocal("/path/to/file", p1)

// Write multiple element
AvroWriter.writeOneToLocal("/path/to/file", list)

```
