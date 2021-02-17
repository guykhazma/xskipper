/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search.expressions

import org.apache.iceberg.expressions.Expression.Operation
import org.apache.iceberg.expressions.{NamedReference, UnboundPredicate, And => AndIceberg, Expression => IcebergExpression, Or => OrIceberg}
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Or}
import org.apache.spark.sql.functions._

/**
  * Translates Iceberg expression to equivalent Spark unresolved expressions
  * TODO: use an abstract expression for xskipper and translate to Spark expression only
  *       when using Spark
  */
object IcebergExpressionTranslator {
  def translate(icebergExpression: IcebergExpression): Expression = {
    icebergExpression match {
      case a: AndIceberg =>
        And(translate(a.left()), translate(a.right()))
      case o: OrIceberg =>
        Or(translate(o.left()), translate(o.right()))
      case e: UnboundPredicate[_] if e.op() == Operation.GT =>
        GreaterThan(col(e.term().asInstanceOf[NamedReference[_]].name).expr,
          lit(e.literal().value()).expr)
      case e: UnboundPredicate[_] if e.op() == Operation.GT_EQ =>
        GreaterThanOrEqual(col(e.term().asInstanceOf[NamedReference[_]].name).expr,
          lit(e.literal().value()).expr)
      case e: UnboundPredicate[_] if e.op() == Operation.LT_EQ =>
        LessThanOrEqual(col(e.term().asInstanceOf[NamedReference[_]].name).expr,
          lit(e.literal().value()).expr)
      case e: UnboundPredicate[_] if e.op() == Operation.LT =>
        LessThan(col(e.term().asInstanceOf[NamedReference[_]].name).expr,
          lit(e.literal().value()).expr)
      case e: UnboundPredicate[_] if e.op() == Operation.EQ =>
        EqualTo(col(e.term().asInstanceOf[NamedReference[_]].name).expr,
          lit(e.literal().value()).expr)
      case _ =>
        // We don't have an equivalent expression so refer to the expression as true
        lit(true).expr
    }
  }

}
