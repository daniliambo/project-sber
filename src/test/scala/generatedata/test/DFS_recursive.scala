package generatedata.test

import scala.collection.mutable.ArrayBuffer

trait DFS_recursive extends App {

  def performDFS(adjList: Seq[Seq[Int]]): (ArrayBuffer[Int], ArrayBuffer[Boolean]) = {
    def generateFalseSeq(size: Int): ArrayBuffer[Boolean] = ArrayBuffer.fill(size)(false)
    val usedList: ArrayBuffer[Boolean]                    = generateFalseSeq(adjList.size)
    val topsort: ArrayBuffer[Int]                         = ArrayBuffer.empty[Int]

    for (v <- adjList.indices)
      if (!usedList(v)) {
        dfs(v)
      }

    def dfs(v: Int): Unit = {
      usedList.update(v, true)
      for (u <- adjList(v))
        if (!usedList(u)) {
          dfs(u)
        }
      topsort.append(v)
    }
    topsort.reverse -> usedList
  }
}
