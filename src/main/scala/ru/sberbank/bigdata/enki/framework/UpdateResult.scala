package ru.sberbank.bigdata.enki.framework

import java.time.LocalDateTime

sealed trait UpdateResult {
  val view: WorkflowTask[_]
}

final case class Updated(override val view: DerivedView[_], start: LocalDateTime, end: LocalDateTime)
    extends UpdateResult

abstract class Skipped(override val view: WorkflowTask[_]) extends UpdateResult

final case class NoChanges(override val view: WorkflowTask[_]) extends Skipped(view)

final case class ExternallyChanged(override val view: WorkflowTask[_]) extends Skipped(view)

final case class SourceSkipped(override val view: WorkflowTask[_]) extends Skipped(view)

abstract class UpdateError(override val view: WorkflowTask[_]) extends UpdateResult

final case class UpdateSelfError(override val view: WorkflowTask[_], error: Throwable) extends UpdateError(view)

final case class UpdateSourceError(override val view: WorkflowTask[_], errorSources: Seq[UpdateError])
    extends UpdateError(view)
