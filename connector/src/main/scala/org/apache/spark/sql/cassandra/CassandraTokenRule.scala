package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.datasource.CassandraTable
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object CassandraTokenRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformDown {
      case filter @ Filter(condition, child) =>
        extractCassandraRelation(child) match {
          case Some((rel, table, options)) =>

            if (hasTokenInOr(condition)) {
              filter
            } else {
              extractTokenBounds(condition, table) match {
                case Some((lowerOpt, upperOpt)) =>
                  val newOptions =
                    withTokenFilterOptions(options, lowerOpt, upperOpt)
                  val newRel = rel.copy(options = newOptions)
                  val newCondition = removeTokenFromCondition(condition)
                  Filter(newCondition, replaceRelation(child, rel, newRel))

                case None =>
                  filter
              }
            }

          case None =>
            filter
        }

      case other => other
    }
  }

  private def extractCassandraRelation(
      plan: LogicalPlan
  ): Option[(DataSourceV2Relation, CassandraTable, CaseInsensitiveStringMap)] =
    plan match {

      case rel @ DataSourceV2Relation(
            table: CassandraTable,
            _,
            _,
            _,
            options: CaseInsensitiveStringMap) =>
        Some((rel, table, options))

      case sa: SubqueryAlias =>
        extractCassandraRelation(sa.child)

      case proj: Project =>
        extractCassandraRelation(proj.child)

      case _ =>
        None
    }

  private def replaceRelation(
      plan: LogicalPlan,
      oldRel: DataSourceV2Relation,
      newRel: DataSourceV2Relation
  ): LogicalPlan =
    plan transformUp {
      case r if r eq oldRel => newRel
    }

  private def withTokenFilterOptions(
      options: CaseInsensitiveStringMap,
      lower: Option[(String, Long)],
      upper: Option[(String, Long)]
  ): CaseInsensitiveStringMap = {
    val baseJavaMap: java.util.Map[String, String] =
      options.asCaseSensitiveMap()
    val newJavaMap = new java.util.HashMap[String, String](baseJavaMap)

    lower.foreach { case (op, v) =>
      newJavaMap.put("cassandra.token.filter.lower", s"$op:$v")
    }
    upper.foreach { case (op, v) =>
      newJavaMap.put("cassandra.token.filter.upper", s"$op:$v")
    }

    new CaseInsensitiveStringMap(newJavaMap)
  }

  private def extractTokenBounds(
      expr: Expression,
      table: CassandraTable
  ): Option[(Option[(String, Long)], Option[(String, Long)])] = {

    def literalToLong(e: Expression): Option[Long] = e match {
      case Literal(v, _) =>
        v match {
          case l: Long    => Some(l)
          case i: Int     => Some(i.toLong)
          case s: Short   => Some(s.toLong)
          case b: Byte    => Some(b.toLong)
          case bi: BigInt => Some(bi.longValue())
          case _          => None
        }
      case Cast(child, _, _, _) =>
        literalToLong(child)
      case _ =>
        None
    }

    def isPkTokenExpr(e: Expression): Boolean = e match {
      case tok: CassandraToken =>
        val pkCols = table.tableDef.partitionKey.map(_.columnName)
        val childColNames = tok.children.collect { case a: Attribute => a.name }
        childColNames == pkCols
      case _ => false
    }

    val allBounds: Seq[(String, Long)] = expr.collect {
      // token(pk) > literal
      case GreaterThan(tok, right)
          if isPkTokenExpr(tok) && literalToLong(right).isDefined =>
        "gt" -> literalToLong(right).get

      // literal > token(pk)  <=>  token(pk) < literal
      case GreaterThan(left, tok)
          if isPkTokenExpr(tok) && literalToLong(left).isDefined =>
        "lt" -> literalToLong(left).get

      // token(pk) >= literal
      case GreaterThanOrEqual(tok, right)
          if isPkTokenExpr(tok) && literalToLong(right).isDefined =>
        "ge" -> literalToLong(right).get

      // literal >= token(pk)  <=>  token(pk) <= literal
      case GreaterThanOrEqual(left, tok)
          if isPkTokenExpr(tok) && literalToLong(left).isDefined =>
        "le" -> literalToLong(left).get

      // token(pk) < literal
      case LessThan(tok, right)
          if isPkTokenExpr(tok) && literalToLong(right).isDefined =>
        "lt" -> literalToLong(right).get

      // literal < token(pk)  <=>  token(pk) > literal
      case LessThan(left, tok)
          if isPkTokenExpr(tok) && literalToLong(left).isDefined =>
        "gt" -> literalToLong(left).get

      // token(pk) <= literal
      case LessThanOrEqual(tok, right)
          if isPkTokenExpr(tok) && literalToLong(right).isDefined =>
        "le" -> literalToLong(right).get

      // literal <= token(pk)  <=>  token(pk) >= literal
      case LessThanOrEqual(left, tok)
          if isPkTokenExpr(tok) && literalToLong(left).isDefined =>
        "ge" -> literalToLong(left).get

      // token(pk) = literal
      case EqualTo(tok, right)
          if isPkTokenExpr(tok) && literalToLong(right).isDefined =>
        "eq" -> literalToLong(right).get

      case EqualTo(left, tok)
          if isPkTokenExpr(tok) && literalToLong(left).isDefined =>
        "eq" -> literalToLong(left).get
    }

    if (allBounds.isEmpty) {
      None
    } else {
      if (allBounds.size > 2) {
        throw new AnalysisException(
          s"Too many token() filters on ${table.tableDef.keyspaceName}.${table.tableDef.tableName}: " +
            s"found ${allBounds.size}, at most 2 are supported (one lower, one upper). " +
            s"Found: ${allBounds.mkString(", ")}"
        )
      }

      val lowers = allBounds.collect {
        case b @ (op, _) if op == "gt" || op == "ge" || op == "eq" => b
      }
      val uppers = allBounds.collect {
        case b @ (op, _) if op == "lt" || op == "le" || op == "eq" => b
      }

      if (lowers.size > 1 || uppers.size > 1) {
        throw new AnalysisException(
          s"Ambiguous token() filters on ${table.tableDef.keyspaceName}.${table.tableDef.tableName}: " +
            s"multiple lower/upper bounds are not supported. " +
            s"Found: ${allBounds.mkString(", ")}"
        )
      }

      val lowerOpt = lowers.headOption
      val upperOpt = uppers.headOption

      if (lowerOpt.isEmpty && upperOpt.isEmpty) None
      else Some((lowerOpt, upperOpt))
    }
  }

  private def removeTokenFromCondition(expr: Expression): Expression = {
    expr transformDown {
      case bc: BinaryComparison
          if bc.left.isInstanceOf[CassandraToken] || bc.right.isInstanceOf[CassandraToken] =>
        Literal.TrueLiteral
    }
  }


  private def hasTokenInOr(expr: Expression): Boolean = {
    expr.exists {
      case Or(left, right) =>
        containsToken(left) || containsToken(right)
      case _ => false
    }
  }

  private def containsToken(expr: Expression): Boolean =
    expr.exists {
      case _: CassandraToken => true
      case _                 => false
    }
}
