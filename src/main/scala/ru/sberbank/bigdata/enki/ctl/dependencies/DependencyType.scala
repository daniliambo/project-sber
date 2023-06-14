package ru.sberbank.bigdata.enki.ctl.dependencies

sealed trait DependencyType

case object Inner  extends DependencyType
case object Outer  extends DependencyType
case object Source extends DependencyType
