package org.apache.samza.sql.test

import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.tools.Frameworks
import org.apache.samza.config.Config
import org.apache.samza.sql.data.IncomingMessageTuple
import org.apache.samza.sql.planner.physical.SamzaRel
import org.apache.samza.sql.planner.{QueryPlannerContext, QueryPlanner}
import org.apache.samza.sql.schema.CalciteModelProcessor
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.task._

class MockSqlTask(schema: String, query: String) extends StreamTask with InitableTask {
  val rootSchema: SchemaPlus = Frameworks.createRootSchema(true)
  val queryContext: QueryPlannerContext = new MockQueryPlannerContext(new CalciteModelProcessor("inline:" + schema, rootSchema).getDefaultSchema)
  val queryPlanner: QueryPlanner = new QueryPlanner(queryContext)
  val operatorRouter = queryPlanner.getOperatorRouter(query)

  override def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator): Unit = {
    operatorRouter.process(new IncomingMessageTuple(envelope), collector, coordinator)
  }

  override def init(config: Config, context: TaskContext): Unit = {
    operatorRouter.init(config, context)
  }
}
