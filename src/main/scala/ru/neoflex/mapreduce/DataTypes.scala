package ru.neoflex.mapreduce

trait DataTypes {

  type ParsedValue
  type Key
  type MappedValue

  type Reduce = (MappedValue, MappedValue) => MappedValue
  type Parse = String => ParsedValue
  type Map = ParsedValue => Seq[(Key, MappedValue)]

}
