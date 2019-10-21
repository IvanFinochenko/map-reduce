package ru.neoflex.mapreduce

import akka.actor.{Actor, ActorLogging, Props}

import scala.collection.mutable

trait Reducer extends DataTypes {

  reduce: Master with Writer =>

  class ReduceExecutor(reduce: Reduce, outputPath: String) extends Actor with ActorLogging {

    private val dataWriter = context.actorOf(DataWriter.props(outputPath))
    private val reducedData = mutable.Map.empty[Key, MappedValue]

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

    override def preStart(): Unit = {
      log.info("ReduceExecutor is starting")
    }

    override def postStop(): Unit = {
      log.info("ReduceExecutor is stopped")
    }

  }

  object ReduceExecutor {
    def props(reduce: Reduce, outputPath: String) = Props(new ReduceExecutor(reduce, outputPath))

    case class ReduceData(key: Key, value: MappedValue)
  }

}
