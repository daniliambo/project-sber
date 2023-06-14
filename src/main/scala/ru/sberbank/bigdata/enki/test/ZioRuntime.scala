package ru.sberbank.bigdata.enki.test

import zio._

private[test] object ZioRuntime {

  lazy val runtime: Runtime[ZEnv] = Runtime.unsafeFromLayer(ZEnv.live)

}
