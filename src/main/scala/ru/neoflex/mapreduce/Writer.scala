package ru.neoflex.mapreduce

import java.io.{File, PrintWriter}

import akka.actor.{Actor, Props}

trait Writer[A, K, B] {

  writer: Master[A, K, B] =>

  class DataWriter(path: String) extends Actor {

    private val file = new PrintWriter(new File(path))

    import DataWriter._
    import MasterExecutor._

    override def receive: Receive = {
      case WriteData(data) => file.println(data.toString)
      case End =>
        file.close()
        sender() ! End
    }
  }

  object DataWriter {

    def props(path: String): Props = Props(new DataWriter(path))

    case class WriteData(data: B)
  }

}

