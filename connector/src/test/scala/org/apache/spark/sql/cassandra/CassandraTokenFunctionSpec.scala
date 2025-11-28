package org.apache.spark.sql.cassandra

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CassandraTokenFunctionSpec extends AnyFunSuite with Matchers {

  test("cassandraTokenFunctionBuilder should require at least one argument") {
    intercept[org.apache.spark.sql.AnalysisException] {
      CassandraMetadataFunction.cassandraTokenFunctionBuilder(Seq.empty)
    }
  }

  test("cassandraTokenFunctionBuilder should build CassandraToken with one argument") {
    val expr = CassandraMetadataFunction.cassandraTokenFunctionBuilder(
      Seq(Literal(1, IntegerType))
    )
    expr shouldBe a[CassandraToken]
    expr.sql shouldBe "TOKEN(1)"
  }

  test("cassandraTokenFunctionBuilder should build CassandraToken with multiple arguments") {
    val expr = CassandraMetadataFunction.cassandraTokenFunctionBuilder(
      Seq(Literal(1, IntegerType), Literal(2, IntegerType))
    )
    expr shouldBe a[CassandraToken]
    expr.sql shouldBe "TOKEN(1, 2)"
  }
}
