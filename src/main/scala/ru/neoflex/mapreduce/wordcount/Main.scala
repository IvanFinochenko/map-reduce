package ru.neoflex.mapreduce.wordcount

import akka.actor.ActorSystem
import ru.neoflex.mapreduce.MapReduceCake

object Main extends App {

  val actorSystem = ActorSystem("word-count")
  val mapReduce = new MapReduceCake[String, String, WordCount]
  val inputpath = "/home/ifinochenko/Projects/courses/map-reduce/src/main/resources/source"
  val outputPath = "/home/ifinochenko/Projects/courses/map-reduce/src/main/resources/output"
  actorSystem.actorOf(mapReduce.MasterExecutor.props(
    identity,
    map,
    (w1, w2) => WordCount(w1.word, w1.count + w2.count),
    inputpath,
    outputPath,
    1,
    1,
    actorSystem
  ))

  private val a = Seq(".", ",", "!", "?", "(", ")", "-", "“", "”",  ":", ";")

  def map(str: String) = {
    str.split(" ").toList
        .map { word =>
          val w = a.foldLeft(word) { case (c, v) => c.replace(v, "") }
          w  -> WordCount(w, 1)
        }
  }



}
