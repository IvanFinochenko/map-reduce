package ru.neoflex.mapreduce

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}

trait Master extends DataTypes {

  master: Reader with Shuffle =>

  import MasterExecutor._

  class MasterExecutor(val operations: Operations, val config: Config, val actorSystem: ActorSystem)
      extends Actor with ActorLogging {

    private var finishedExecutors = 0
    private var processedPartitions = 0

    private val shuffleExecutor = context.actorOf(
      ShuffleExecutor.props(config.countReducers, operations.reduce, config.outputPath)
    )
    private val source = new File(config.inputPath)
    private val partitions = getListOfFiles(source)
    private val dataReaders = partitions
        .take(config.countMappers)
        .map(file => context.actorOf(DataReader.props(operations.parse, file, operations.map, shuffleExecutor)))

    import DataReader._

    override def receive: Receive = {
      case Free(dataReader) =>
        log.info(s"$dataReader is free")
        processedPartitions = processedPartitions + 1
        if (processedPartitions == partitions.size) {
          dataReaders.foreach(_ ! End)
        } else {
          dataReader ! NextPartition(partitions(processedPartitions))
        }
      case End =>
        finishedExecutors = finishedExecutors + 1
        if (finishedExecutors == config.countReducers) {
          log.info("Job finished successfully")
          actorSystem.terminate()
        }
    }

    private def getListOfFiles(dir: File): Vector[File] = dir.listFiles.filter(_.isFile).toVector

  }

  object MasterExecutor {

    case class Operations(parse: Parse, map: Map, reduce: Reduce)

    case class Config(inputPath: String, outputPath: String, countMappers: Int, countReducers: Int)

    def props(operations: Operations, config: Config, actorSystem: ActorSystem): Props = {
      Props(new MasterExecutor(operations, config, actorSystem))
    }

    case object End
  }
}


