package ru.neoflex.mapreduce

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import scala.collection.mutable.ArrayBuffer

trait Shuffle extends DataTypes {

  shuffle: Master with Reducer =>

  class ShuffleExecutor(
      val countReducers: Int,
      val reduce: Reduce,
      outputPath: String) extends Actor with ActorLogging {

    private var keysReducers = ArrayBuffer.empty[(Set[Key], ActorRef)]
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

    override def preStart(): Unit = {
      log.info("ShuffleExecutor is starting")
    }

    override def postStop(): Unit = {
      log.info("ShuffleExecutor is stopped")
    }

  }

  case object ShuffleExecutor {

    def props(countReducers: Int, reduce: Reduce, outputPath: String): Props = {
      Props(new ShuffleExecutor(countReducers, reduce, outputPath))
    }

    case class ShuffleData(key: Key, value: MappedValue)
  }

}
