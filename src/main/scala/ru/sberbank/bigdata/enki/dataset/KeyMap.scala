package ru.sberbank.bigdata.enki.dataset

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import ru.sberbank.bigdata.enki.schema._

object KeyMap {

  /** Replace surrogate keys in keyMap when join condition satisfied (natural keys matched).
    * Natural keys in key source should be unique, otherwise records in key map will be duplicated.
    *
    * @param keyMap           Key map with keys to be raplaced
    * @param keySource        Source of surrogate keys
    * @param joinCondition    Join conditions (matching natural keys)
    * @param mapSurrogateKeys Surrogate key colums in key map (key order should match srcSurrogateKeys)
    * @param srcSurrogateKeys Surrogate key colums in key source (key order should match mapSurrogateKeys)
    * @tparam T type of the key map
    * @return Key map with surrogate keys replaced.
    */
  def updateSurrogateKeys[T: Encoder](
    keyMap: Dataset[T],
    keySource: Dataset[_],
    joinCondition: Column,
    mapSurrogateKeys: Seq[Column],
    srcSurrogateKeys: Seq[Column]
  ): Dataset[T] = {
    import keyMap.sparkSession.implicits._

    if (mapSurrogateKeys.length != srcSurrogateKeys.length)
      throw new Exception("Surrogate key colums count does not match.")

    val mapSurrogateKeyNames  = mapSurrogateKeys.map(columnName)
    val mapSurrogateKeySet    = mapSurrogateKeyNames.toSet
    val srcSurrogateKeysNames = srcSurrogateKeys.map(columnName)
    val mapPreservedColumns   = keyMap.schema.map(_.name).filter(!mapSurrogateKeySet.contains(_))

    keyMap
      .join(keySource.withColumn("__joined", lit(true)), joinCondition, "left")
      .select(
        mapPreservedColumns.map(keyMap(_))
          ++ srcSurrogateKeysNames
            .zip(mapSurrogateKeyNames)
            .map(p => when($"__joined", keySource(p._1)).otherwise(keyMap(p._2)).as(p._2)): _*
      )
      .as[T]
  }
}
