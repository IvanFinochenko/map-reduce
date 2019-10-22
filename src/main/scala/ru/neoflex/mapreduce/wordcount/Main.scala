package ru.neoflex.mapreduce.wordcount

import akka.actor.ActorSystem
import ru.neoflex.mapreduce.MapReduce

object Main extends App {

  val actorSystem = ActorSystem("word-count")
  val mapReduce = new MapReduce[String, String, WordCount]
  val inputPath = "/home/ivan/Projects/map-reduce/src/main/resources/source"
  val outputPath = "/home/ivan/Projects/map-reduce/src/main/resources/output"
  val wordCountOperations = mapReduce.MasterExecutor.Operations(identity, map, reduce)
  val config = mapReduce.MasterExecutor.Config(inputPath, outputPath, 3, 2)

  mapReduce.MasterExecutor
      .props(wordCountOperations, config, actorSystem)
      .fold(
        { errorMessage =>
          actorSystem.log.error(errorMessage)
          actorSystem.terminate()
        },
        masterExecutor => actorSystem.actorOf(masterExecutor)
      )


  private val punctuationMarks = Seq(".", ",", "!", "?", "(", ")", "-", "“", "”",  ":", ";")

  def map(str: String): Seq[(String, WordCount)] = {
    str
        .split(" ")
        .toList
        .map { word =>
          val pureWord = punctuationMarks.foldLeft(word) { case (w, mark) => w.replace(mark, "") }
          pureWord -> WordCount(pureWord, 1)
        }
  }

  def reduce(wordCount1: WordCount, wordCount2: WordCount): WordCount = {
    WordCount(wordCount1.word, wordCount1.count + wordCount2.count)
  }

}
