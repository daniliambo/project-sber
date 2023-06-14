package generatedata.test

import org.scalacheck.{Gen, Prop}
import scala.collection.mutable.ArrayBuffer

object GenerateGraph_ extends DFS_recursive with App {

  def genGraphGenerator(nGenerator: Gen[Int]): Gen[Seq[Seq[Int]]] =
    for {
      n <- nGenerator
      seq = (0 until n).map(genVertex(_, n))
      gen <- Gen.sequence[Seq, Seq[Int]](seq)
    } yield gen

  val nGenerator: Gen[Int] = Gen.choose(min = 10, max = 10)

  def genVertex(v: Int, n: Int): Gen[Seq[Int]] = {
    val validVertices = (v + 1) until n
    val gen           = Gen.someOf(validVertices)
    gen
  }
  val graph = genGraphGenerator(nGenerator).sample.get
  println(graph)

//  val usedList1 = DFS_iterative.dfs(graph)
//  val dfsResult = DFS_iterative.dfs(graph)

  val dfsProp = Prop.forAll(genGraphGenerator(nGenerator)) { graph =>
    val result: (ArrayBuffer[Int], ArrayBuffer[Boolean]) = performDFS(graph)
    val sortedVertices                                   = result._1
    val usedVertices                                     = result._2
    println(s"$sortedVertices => Sorted Vertices")
    usedVertices.forall(_ == true)
  }

  val numOfTests = 1
  dfsProp.check(_.withMinSuccessfulTests(numOfTests))

}
