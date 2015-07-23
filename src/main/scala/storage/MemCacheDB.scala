package storage

class MemCacheDB[K,V] {

  val db =  scala.collection.mutable.HashMap.empty[K, V]

  def get(key: K): Option[V] = {
    db.get(key)
  }

  def put(key: K, value: V) = {
    db += (key -> value)
  }

  def remove(key: K): Option[V] ={
    db.remove(key)
  }

}