package ru.sberbank.bigdata.enki.framework

import cats.effect.{Blocker, ContextShift, ExitCase, IO, Timer}
import cats.implicits.catsSyntaxFlatMapOps
import org.apache.spark.sql.SparkSession

import java.util.concurrent.Executors
import scala.concurrent._
import scala.concurrent.duration.FiniteDuration
import scala.sys.process._

abstract class PythonScript extends CustomAction[Unit] {
  import PythonScript._

  def script: String

  def envVars: Map[String, String] = Map.empty

  def timeout: Option[FiniteDuration] = None

  def pythonPath: Option[String] = None

  override def execute(args: Array[String])(implicit spark: SparkSession): Unit = {
    val python = pythonPath.getOrElse("python")
    val pb = Process(
      Seq(python, "-c", script) ++ args,
      None,
      envVars.toSeq: _*
    )

    val process = blocking(pb.run(mkProcessLogger)).bracketCase { p =>
      blocking(p.exitValue()).flatMap {
        case 0 => IO.unit
        case _ => logAndRethrow(new RuntimeException("Script returned non-zero exit code"))
      }
    } { (p, exit) =>
      exit match {
        case ExitCase.Completed                    => IO.unit
        case ExitCase.Error(_) | ExitCase.Canceled => blocking(p.destroy())
      }
    }

    val task = timeout match {
      case Some(t) =>
        process.timeoutTo(t, logAndRethrow(new TimeoutException(s"Script timed out after $t")))
      case None =>
        process
    }

    task.unsafeRunSync()
  }

  private def mkProcessLogger: ProcessLogger = ProcessLogger(info, error)

  private def blocking[A](thunk: => A): IO[A] = blocker.delay[IO, A](thunk)

  private def logAndRethrow(exception: Exception): IO[Unit] =
    IO(error(exception.getMessage)) >> IO.raiseError(exception)

}

private[enki] object PythonScript {
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO]     = IO.timer(ExecutionContext.global)

  val blocker: Blocker = Blocker.liftExecutorService(Executors.newCachedThreadPool())
}
