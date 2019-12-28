package ru.neoflex.mapreduce

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, MustMatchers}

class MapSpec
    extends TestKit(ActorSystem("word-count"))
        with FlatSpecLike
        with BeforeAndAfterAll
        with MustMatchers
        with ImplicitSender {

  "MapExecutor" should "apply map function" in {
    val shuffle = TestProbe()
    val key = "key"
    val map: Int => Seq[(String, Double)] = x => Seq(key -> x / 15d)
    val handler = new MapTest
    val mapExecutor = system.actorOf(handler.MapExecutor.props(map, shuffle.ref))
    val data = 10
    mapExecutor ! handler.MapExecutor.Data(data)
    val expectMessage = handler.ShuffleExecutor.ShuffleData(key, data / 15d)
    shuffle.expectMsg(expectMessage)
  }

  private class MapTest extends Map with Reader with Master with Shuffle with Reducer with Writer {

    override type ParsedValue = Int
    override type Key = String
    override type MappedValue = Double
  }
}
