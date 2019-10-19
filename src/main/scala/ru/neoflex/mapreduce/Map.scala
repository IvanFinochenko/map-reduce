package ru.neoflex.mapreduce

import akka.actor.{Actor, ActorRef, Props}

trait Map extends DataTypes {

  self: Reader with Master with Shuffle =>

  class MapExecutor(map: Map, shuffle: ActorRef) extends Actor {

    import MapExecutor._
    import DataReader._
    import MasterExecutor._
    import ShuffleExecutor._

    override def receive: Receive = {
      case Data(data) => map(data).foreach { case (key, value) => shuffle ! ShuffleData(key, value) }
      case Last => context.parent ! LastDone
      case End => shuffle.forward(End)
    }

  }

  object MapExecutor {
    case class Data(data: ParsedValue)
    case object LastDone

    def props(map: Map, reduceRouter: ActorRef): Props = {
      Props(new MapExecutor(map, reduceRouter))
    }
  }
}

