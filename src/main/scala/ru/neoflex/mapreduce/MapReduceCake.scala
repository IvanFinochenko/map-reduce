package ru.neoflex.mapreduce

class MapReduceCake[A, K, B] extends Master
    with Shuffle
    with Map
    with Reader
    with Reducer
    with Writer {
  override type ParsedValue = A
  override type Key = K
  override type MappedValue = B
}
