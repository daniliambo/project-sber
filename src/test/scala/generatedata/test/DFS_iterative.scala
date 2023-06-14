package generatedata.test

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait DFS_iterative extends App {

  def generateFalseSeq(size: Int): ArrayBuffer[Boolean] = ArrayBuffer.fill(size)(false)
  val adjList: Seq[Seq[Int]]                            = Seq(Seq(1, 4), Seq(2, 3), Seq(), Seq(), Seq(5), Seq())

  def dfs(adjList: Seq[Seq[Int]]): ArrayBuffer[Boolean] = {
    val usedList: ArrayBuffer[Boolean] = generateFalseSeq(adjList.size)
    val stack: mutable.Stack[Int]      = mutable.Stack()

    for (i <- adjList.indices)
      if (!usedList(i)) {
        val v: Seq[Int] = adjList(i)
        stack.pushAll(v)
        usedList.update(i, true)

        while (stack.nonEmpty) {
          val v: Int = stack.pop()
          usedList.update(v, true)

          val u: Seq[Int] = adjList(v)
          for (c <- u)
            if (!usedList(c)) {
              stack.push(c)
            }
        }
      }
    usedList
  }
}
