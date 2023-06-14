package generatedata.unittest

import generatedata.getters.GetTables
import generatedata.graph.constraint.Constraint
import generatedata.modules.module1.S3
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.sberbank.bigdata.enki.business.Checker
import ru.sberbank.bigdata.enki.plan.nodes.{Node, SourceTableNode}

import scala.collection.mutable

class UnitTest extends AnyWordSpec with Matchers with GetTables {

  "Checker" should {

    "Check Data" in {
//      val sortedGraph = setUp(S3)
      val generatedData = main(S3)
      generatedData.foreach { case (source, df) =>
        println(source.fullName + "\n\n")

        df.show
        println("\n\n")
      }
    }
  }

}
