package ru.neoflex.mapreduce

import akka.actor.{Actor, ActorRef, Props}

import scala.collection.mutable.ArrayBuffer

trait Shuffle[A, K, B] {

  shuffle: Master[A, K, B] with Reduce[A, K, B] =>

  class ShuffleExecutor(val countReducers: Int, val reduce: (B, B) => B, outputPath: String) extends Actor {

    private var keysReducers = ArrayBuffer.empty[(Set[K], ActorRef)]
    private var currentReducer: Int = 0

    import ShuffleExecutor._
    import MasterExecutor._
    import ReduceExecutor._

    override def receive: Receive = {
      case ShuffleData(key, value) =>
        val reducedData = ReduceData(key, value)
        keysReducers.find(_._1.contains(key)) match {
          case Some((_, reducer)) =>
            reducer ! reducedData
          case None if keysReducers.size < countReducers =>
            val outputPartitionPath = s"$outputPath/partition-${keysReducers.size}.txt"
            val newReducer = context.actorOf(ReduceExecutor.props(reduce, outputPartitionPath))
            keysReducers = keysReducers :+ Set(key) -> newReducer
            newReducer ! reducedData
          case None =>
            val (keys, reducer) = keysReducers(currentReducer)
            keysReducers(currentReducer) = (keys + key) -> reducer
            reducer ! reducedData
            currentReducer = if (currentReducer < keysReducers.size - 1) currentReducer + 1 else 0
        }
      case End => keysReducers.foreach { case (_, reducer) => reducer.forward(End) }
    }
  }

  case object ShuffleExecutor {

    def props(countReducers: Int, reduce: (B, B) => B, outputPath: String): Props = {
      Props(new ShuffleExecutor(countReducers, reduce, outputPath))
    }

    case class ShuffleData(key: K, value: B)
  }

}
