package ru.sberbank.bigdata.enki.plan.util

/** Used to generate new names and store already used ones */
final class NameContext(private val seed: Int, private val alreadyUsed: Map[String, Int]) {

  def exists(name: String): Boolean = alreadyUsed.contains(name)

  def update(name: String): (NameContext, String) = {
    val (updatedMap, updatedName) = updateUsedNames(name)
    (new NameContext(seed, updatedMap), updatedName)
  }

  def generateName: (NameContext, String) = {
    val candidate          = nameFromSeed(seed)
    val (updatedMap, name) = updateUsedNames(candidate)
    (new NameContext(seed + 1, updatedMap), name)
  }

  private def updateUsedNames(name: String): (Map[String, Int], String) = {
    val id          = alreadyUsed.getOrElse(name, 0)
    val updatedName = if (id == 0) name else s"$name$id"
    val updatedMap  = alreadyUsed + ((name, id + 1))

    (updatedMap, updatedName)
  }

  private def nameFromSeed(seed: Int): String = {
    val letters = 'a' to 'z'
    letters(seed % 26).toString
  }

}

object NameContext {
  def empty: NameContext = new NameContext(0, Map.empty)

  def fromNames(names: Seq[String]): NameContext = new NameContext(0, names.map(_ -> 1).toMap)
}
