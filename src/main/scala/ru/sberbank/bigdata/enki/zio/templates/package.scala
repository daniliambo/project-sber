package ru.sberbank.bigdata.enki.zio

import zio._

/** Templates for programs using ZIO
  * Intended for code reuse and writing tests
  */
package object templates {

  /** Template for application which finishes with the end of main
    * and monitor running potentially throwing exceptions, that can kill all application
    *
    * @param main Main application logic
    * @param monitor Logic repeated in background
    * @param schedule Interval with which monitor logic is repeated
    * @return ZIO application
    */
  def mainAndMonitor[R, E1, E2, A1, A2, B](
    main: ZIO[R, E1, A1],
    monitor: ZIO[R, E2, A2],
    schedule: Schedule[R, A2, B]
  ): ZIO[R, Any, A1] =
    for {
      monitorFiber <- monitor.repeat(schedule).fork // Run monitoring on background
      result       <- main // Run main task an return result of it
      _            <- monitorFiber.interrupt // Ensure monitoring finish after main class
    } yield result

}
