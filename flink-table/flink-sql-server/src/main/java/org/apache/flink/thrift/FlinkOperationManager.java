/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.thrift;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.operation.GetFunctionsOperation;
import org.apache.hive.service.cli.operation.GetSchemasOperation;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.session.HiveSession;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FlinkOperationManager.
 */
public class FlinkOperationManager extends OperationManager {

	// Get value from parent
	final ConcurrentHashMap<OperationHandle, Operation> handleToOperation =
		(ConcurrentHashMap<OperationHandle, Operation>) ReflectionUtils.getSuperField(this, 1, "handleToOperation");
	final ConcurrentHashMap<SessionHandle, FlinkSession> handleToSession = new ConcurrentHashMap<>();

	@Override
	public ExecuteStatementOperation newExecuteStatementOperation(HiveSession parentSession,
			String statement, Map<String, String> confOverlay, boolean runAsync, long queryTimeout)
		throws HiveSQLException {
		FlinkSession flinkSession = handleToSession.get(parentSession.getSessionHandle());
		ExecuteStatementOperation executeStatementOperation =
			new FlinkExecuteStatementOperation(flinkSession.getExecutor(), parentSession, statement, confOverlay, runAsync);
		handleToOperation.put(executeStatementOperation.getHandle(), executeStatementOperation);
		return executeStatementOperation;
	}

	@Override
	public GetSchemasOperation newGetSchemasOperation(HiveSession parentSession, String catalogName, String schemaName) {
		FlinkSession flinkSession = handleToSession.get(parentSession.getSessionHandle());
		GetSchemasOperation operation = new FlinkGetSchemasOperation(flinkSession.getExecutor(), parentSession, catalogName, schemaName);
		handleToOperation.put(operation.getHandle(), operation);
		return operation;
	}

	@Override
	public GetFunctionsOperation newGetFunctionsOperation(HiveSession parentSession, String catalogName, String schemaName, String functionName) {
		FlinkSession flinkSession = handleToSession.get(parentSession.getSessionHandle());
		GetFunctionsOperation operation = new FlinkGetFunctionsOperation(flinkSession.getExecutor(), parentSession,
			catalogName, schemaName, functionName);
		handleToOperation.put(operation.getHandle(), operation);
		return operation;
	}

}
