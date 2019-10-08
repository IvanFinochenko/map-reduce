package ru.neoflex.mapreduce

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}

trait Master[A, K, B] {

  master: Reader[A, K, B] with Shuffle[A, K, B] =>

  class MasterExecutor(
      parse: String => A,
      map: A => Seq[(K, B)],
      reduce: (B, B) => B,
      inputPath: String,
      outputPath: String,
      countMappers: Int,
      countReducers: Int,
      actorSystem: ActorSystem) extends Actor with ActorLogging {

    private var finishedExecutors = 0
    private var processedPartitions = 0

    private val shuffleExecutor = context.actorOf(ShuffleExecutor.props(countReducers, reduce, outputPath))
    private val source = new File(inputPath)
    private val partitions = getListOfFiles(source)
    private val dataReaders = partitions
        .take(countMappers)
        .map(file => context.actorOf(DataReader.props(parse, file, map, shuffleExecutor)))

    import MasterExecutor._
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
        if (finishedExecutors == countReducers) {
          log.info("Job finished successfully")
          actorSystem.terminate()
        }
    }

    private def getListOfFiles(dir: File): Vector[File] = dir.listFiles.filter(_.isFile).toVector
  }

  object MasterExecutor {

    def props(
        parse: String => A,
        map: A => Seq[(K, B)],
        reduce: (B, B) => B,
        inputPath: String,
        outputPath: String,
        countMappers: Int,
        countReducers: Int,
        actorSystem: ActorSystem): Props = {
      Props(new MasterExecutor(parse, map, reduce, inputPath, outputPath, countMappers, countReducers, actorSystem))
    }

    case object End
  }
}


