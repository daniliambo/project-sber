package ru.sberbank.bigdata.enki.sql

trait OptionColumnRelation extends IdentityColumnRelation {
  implicit def option[T[_] <: Option[_], R]: TypeRelation[T[R], R] = new TypeRelation[T[R], R] {}
}
