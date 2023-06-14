package ru.sberbank.bigdata.enki.framework

sealed trait WriteMode

object Append extends WriteMode

object Overwrite extends WriteMode
