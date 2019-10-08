package ru.neoflex.mapreduce

class MapReduceCake[A, K, B] extends Master[A, K, B]
    with Shuffle[A, K, B]
    with Map[A, K, B]
    with Reader[A, K, B]
    with Reduce[A, K, B]
    with Writer[A, K, B]
