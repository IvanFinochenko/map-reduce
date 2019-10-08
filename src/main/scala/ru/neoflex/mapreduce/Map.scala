package ru.neoflex.mapreduce

import akka.actor.{Actor, ActorRef, Props}

trait Map[A, K, B] {

  self: Reader[A, K, B] with Master[A, K, B] with Shuffle[A, K, B] =>

  class MapExecutor(map: A => Seq[(K, B)], shuffle: ActorRef) extends Actor {

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
    case class Data(data: A)
    case object LastDone

    def props(map: A => Seq[(K, B)], reduceRouter: ActorRef): Props = {
      Props(new MapExecutor(map, reduceRouter))
    }
  }
}

