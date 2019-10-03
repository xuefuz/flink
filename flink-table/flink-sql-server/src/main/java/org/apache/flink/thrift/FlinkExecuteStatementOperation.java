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

import org.apache.flink.table.api.Table;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.server.gateway.Executor;
import org.apache.flink.table.server.gateway.ResultDescriptor;
import org.apache.flink.table.server.gateway.TypedResult;
import org.apache.flink.types.Row;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.session.HiveSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * FlinkExecuteStatementOperation.
 */
public class FlinkExecuteStatementOperation extends ExecuteStatementOperation {
	private Executor executor;
	private Table table;

	private ResultDescriptor resultDesc;

	public FlinkExecuteStatementOperation(Executor executor, HiveSession parentSession, String statement,
									Map<String, String> confOverlay, boolean runInBackground) {
		super(parentSession, statement, confOverlay, runInBackground);
		this.executor = executor;
	}

	@Override
	protected void runInternal() throws HiveSQLException {
		setState(OperationState.RUNNING);
		resultDesc = executor.executeQuery(statement);
		setState(OperationState.FINISHED);
		setHasResultSet(true);
	}

	@Override
	public void cancel(OperationState operationState) throws HiveSQLException {
	}

	@Override
	public void close() throws HiveSQLException {

	}

	@Override
	public TableSchema getResultSetSchema() throws HiveSQLException {
		List<FieldSchema> columns = HiveTableUtil.createHiveColumns(resultDesc.getResultSchema());
		return new TableSchema(columns);
	}

	@Override
	public RowSet getNextRowSet(FetchOrientation fetchOrientation, long l) throws HiveSQLException {
		List<Row> rows = new ArrayList<>();
		while (true) {
			TypedResult<Integer> result = executor.snapshotResult(resultDesc.getResultId(), 100);
			if (result.getType() == TypedResult.ResultType.EOS) {
				break;
			} else if (result.getType() == TypedResult.ResultType.PAYLOAD) {
				int pageCount = result.getPayload();
				for (int page = 1; page < pageCount + 1; page++) {
					rows.addAll(executor.retrieveResultPage(resultDesc.getResultId(), page));
				}
			} else {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		RowSet rowSet = RowSetFactory.create(getResultSetSchema(), getProtocolVersion(), false);
		for (Row row : rows) {
			rowSet.addRow(row.getFields());
		}
		return rowSet;
	}

}
