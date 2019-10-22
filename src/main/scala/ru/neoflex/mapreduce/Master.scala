package ru.neoflex.mapreduce

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}

trait Master extends DataTypes {

  master: Reader with Shuffle =>

  import MasterExecutor._

  class MasterExecutor(
      val operations: Operations,
      val config: Config,
      val actorSystem: ActorSystem,
      val partitions: Vector[File]) extends Actor with ActorLogging {

    private var finishedExecutors = 0
    private var sentPartitions = config.countMappers

    private val shuffleExecutor = context.actorOf(
      ShuffleExecutor.props(config.countReducers, operations.reduce, config.outputPath)
    )

    private val dataReaders = partitions
        .take(config.countMappers)
        .zipWithIndex
        .map { case (file, index) =>
          val name = s"DataReader-$index"
          context.actorOf(DataReader.props(operations.parse, file, operations.map, shuffleExecutor), name)
        }

    import DataReader._

    override def receive: Receive = {
      case Free(dataReader) =>
        log.info(s"$dataReader is free")
        if (sentPartitions == partitions.size) {
          log.info("Send all data readers End")
          dataReaders.foreach(_ ! End)
        } else {
          val nextPartition = partitions(sentPartitions)
          log.info(s"send next patition = $nextPartition to $dataReader")
          dataReader ! NextPartition(nextPartition)
          sentPartitions = sentPartitions + 1
        }
      case End =>
        finishedExecutors = finishedExecutors + 1
        if (finishedExecutors == config.countReducers) {
          log.info("Job finished successfully")
          actorSystem.terminate()
        }
    }

    override def preStart(): Unit = {
      log.info("Master executor is starting")
    }

    override def postStop(): Unit = {
      log.info("Master executor is stopped")
    }

  }

  object MasterExecutor {

    case class Operations(parse: Parse, map: Map, reduce: Reduce)

    case class Config(inputPath: String, outputPath: String, countMappers: Int, countReducers: Int)

    def props(operations: Operations, config: Config, actorSystem: ActorSystem): Either[String, Props] = {
      for {
        partitions <- getListOfFiles(config.inputPath).map(_.sorted)
        _ <- if (new File(config.outputPath).exists()) {
          Right()
        } else {
          Left(s"Output Path ${config.outputPath} is not exists")
        }
      } yield {
        Props(new MasterExecutor(operations, config, actorSystem, partitions))
      }
    }

    private def getListOfFiles(inputPath: String): Either[String, Vector[File]] = {
      val dir = new File(inputPath)
      if (dir.exists()) {
        Right(dir.listFiles.filter(_.isFile).toVector)
      } else {
        Left(s"Input Path $inputPath is not exists")
      }
    }

    case object End
  }
}


