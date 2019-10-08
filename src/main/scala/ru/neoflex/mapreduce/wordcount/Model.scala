package ru.neoflex.mapreduce.wordcount

case class WordCount(word: String, count: Long) {
  override def toString: String = s"$word, $count"
}
