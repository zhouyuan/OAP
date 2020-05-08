package com.intel.sparkColumnarPlugin

import com.intel.sparkColumnarPlugin.execution._
import com.intel.sparkColumnarPlugin.expression._
import org.apache.spark.sql.catalyst.expressions._

import scala.util.control.Breaks._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.RowToColumnarExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

case class ColumnarPreOverrides() extends Rule[SparkPlan] {

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: BatchScanExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new ColumnarBatchScanExec(plan.output, plan.scan)
    case plan: ProjectExec =>
      //new ColumnarProjectExec(plan.projectList, replaceWithColumnarPlan(plan.child))
      val columnarPlan = replaceWithColumnarPlan(plan.child)
      val res = if (!columnarPlan.isInstanceOf[ColumnarConditionProjectExec]) {
        val supp = checklist(plan.projectList)
        if (supp) {
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          new ColumnarConditionProjectExec(null, plan.projectList, columnarPlan)
        } else {
          logDebug(s"Columnar Processing for ${plan.getClass} is currently not supported due to unsupported function in sub tasks")
          plan
        }
      } else {
        val cur_plan = columnarPlan.asInstanceOf[ColumnarConditionProjectExec]
        val supp = check(cur_plan.condition)
        if (supp) {
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          new ColumnarConditionProjectExec(cur_plan.condition, plan.projectList, cur_plan.child)
        } else {
          logDebug(s"Columnar Processing for ${plan.getClass} is currently not supported due to unsupported function in sub tasks")
          plan
        }
      }
      res
    case plan: FilterExec =>
      val child = replaceWithColumnarPlan(plan.child)
      val supp = check(plan.condition)
      if (supp) {
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        new ColumnarConditionProjectExec(plan.condition, null, child)
      } else {
        logDebug(s"Columnar Processing for ${plan.getClass} is currently not supported due to unsupported function in sub tasks")
        plan
      }
    case plan: HashAggregateExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new ColumnarHashAggregateExec(
        plan.requiredChildDistributionExpressions,
        plan.groupingExpressions,
        plan.aggregateExpressions,
        plan.aggregateAttributes,
        plan.initialInputBufferOffset,
        plan.resultExpressions,
        child)
    /*case plan: SortExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new ColumnarSortExec(
        plan.sortOrder,
        plan.global,
        child,
        plan.testSpillFrequency)*/
    /*case plan: ShuffleExchangeExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new ColumnarShuffleExchangeExec(
        plan.outputPartitioning,
        replaceWithColumnarPlan(plan.child),
        plan.canChangeNumPartitions)*/
    case plan: ShuffledHashJoinExec =>
      val left = replaceWithColumnarPlan(plan.left)
      val right = replaceWithColumnarPlan(plan.right)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      val res = new ColumnarShuffledHashJoinExec(
        plan.leftKeys,
        plan.rightKeys,
        plan.joinType,
        plan.buildSide,
        plan.condition,
        left,
        right)
      res
    case p =>
      val children = p.children.map(replaceWithColumnarPlan)
      logDebug(s"Columnar Processing for ${p.getClass} is not currently supported.")
      p.withNewChildren(children)
  }

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithColumnarPlan(plan)
  }

  def checklist(exprList: Seq[Expression]): Boolean = {
    var supported :Boolean = true
    for (expr <- exprList if(!check(expr))) {
      supported = false
      break
    }
    supported
  }

  def check(condExpr: Expression): Boolean = {
    var supported :Boolean = true
    try {
      ColumnarExpressionConverter.replaceWithColumnarExpression(condExpr, null)
    } catch {
      case e: Exception => supported = false
    }
    supported
  }

}

case class ColumnarPostOverrides() extends Rule[SparkPlan] {
  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: RowToColumnarExec =>
      val child = replaceWithColumnarPlan(plan.child)
      RowToArrowColumnarExec(child)
    case p =>
      val children = p.children.map(replaceWithColumnarPlan)
      p.withNewChildren(children)
  }

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithColumnarPlan(plan)
  }
}

case class ColumnarOverrideRules(session: SparkSession) extends ColumnarRule with Logging {
  def columnarEnabled =
    session.sqlContext.getConf("org.apache.spark.example.columnar.enabled", "true").trim.toBoolean
  val preOverrides = ColumnarPreOverrides()
  val postOverrides = ColumnarPostOverrides()

  override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      preOverrides(plan)
    } else {
      plan
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      postOverrides(plan)
    } else {
      plan
    }
  }
}

/**
 * Extension point to enable columnar processing.
 *
 * To run with columnar set spark.sql.extensions to com.intel.sparkColumnarPlugin.ColumnarPlugin
 */
class ColumnarPlugin extends Function1[SparkSessionExtensions, Unit] with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logWarning(
      "Installing extensions to enable columnar CPU support." +
        " To disable this set `org.apache.spark.example.columnar.enabled` to false")
    extensions.injectColumnar((session) => ColumnarOverrideRules(session))
  }
}
