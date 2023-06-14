package ru.sberbank.bigdata.enki.test

import zio._

trait ZioTestRuntime {

  implicit lazy val runtime: Runtime[ZEnv] = ZioRuntime.runtime

}
