package ru.neoflex.mapreduce

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import scala.io.{BufferedSource, Source}

trait Reader[A, K, B] {

  data: Map[A, K, B] with Master[A, K, B] =>

  class DataReader(parse: String => A, initPartition: File, map: A => Seq[(K, B)], shuffle: ActorRef) extends Actor with ActorLogging {

    import DataReader._
    import MapExecutor._
    import MasterExecutor._

    private val mapExecutor = context.actorOf(MapExecutor.props(map, shuffle))

    readFileAndSendData(initPartition)

    override def receive: Receive = {
      case NextPartition(file) =>
        readFileAndSendData(file)
      case LastDone => context.parent ! Free(self)
      case End => mapExecutor.forward(End)
    }

    private def readFileAndSendData(file: File): Unit = {
      log.info(s"Start reading ${file.getCanonicalPath}")
      val source = Source.fromFile(file)
      source.getLines().foreach(x => mapExecutor ! Data(parse(x)))
      mapExecutor ! Last
      source.close()
    }

  }

  case object DataReader {

    def props(parse: String => A, initPath: File, map: A => Seq[(K, B)], shuffle: ActorRef): Props = {
      Props(new DataReader(parse, initPath, map, shuffle))
    }

    case class NextPartition(path: File)
    case class Free(dataReader: ActorRef)
    case object Last
  }

}

