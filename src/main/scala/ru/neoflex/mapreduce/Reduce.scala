package ru.neoflex.mapreduce

import akka.actor.{Actor, Props}

import scala.collection.mutable

trait Reduce[A, K, B] {

  reduce: Master[A, K, B] with Writer[A, K, B] =>

  class ReduceExecutor(reduce: (B, B) => B, outputPath: String) extends Actor {

    private val dataWriter = context.actorOf(DataWriter.props(outputPath))
    private val reducedData = mutable.Map.empty[K, B]

    import MasterExecutor._
    import ReduceExecutor._
    import DataWriter._

    override def receive: Receive = {
      case ReduceData(key, value) =>
        val reducedValue = reducedData
            .get(key)
            .map(x => reduce(x, value))
            .getOrElse(value)
        reducedData.update(key, reducedValue)
      case End =>
        reducedData.foreach { case (_, data) => dataWriter ! WriteData(data) }
        dataWriter.forward(End)
    }

  }

  object ReduceExecutor {
    def props(reduce: (B, B) => B, outputPath: String) = Props(new ReduceExecutor(reduce, outputPath))

    case class ReduceData(key: K, value: B)
  }

}
