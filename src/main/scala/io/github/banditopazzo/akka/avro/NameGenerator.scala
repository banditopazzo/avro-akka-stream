package io.github.banditopazzo.akka.avro

trait NameGenerator {

  def generate(): String

}

object NameGenerator {

  class DefaultNameGenerator extends NameGenerator {

    private var counter: Int = 0

    def generate(): String = {
      val out = "%010d".format(counter)
      counter += 1
      s"part-$out.avro"
    }

  }

}
