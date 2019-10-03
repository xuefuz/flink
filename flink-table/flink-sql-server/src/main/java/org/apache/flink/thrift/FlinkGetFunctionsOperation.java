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

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.server.gateway.Executor;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.operation.GetFunctionsOperation;
import org.apache.hive.service.cli.session.HiveSession;

import java.sql.DatabaseMetaData;
import java.util.Optional;

/**
 * FlinkGetFunctionsOperation.
 */
public class FlinkGetFunctionsOperation extends GetFunctionsOperation {
	private Executor executor;

	private String catalogName;
	private String databaseName;
	private String functionName;

	public FlinkGetFunctionsOperation(Executor executor, HiveSession parentSession, String catalogName, String schemaName, String functionName) {
		super(parentSession, catalogName, schemaName, functionName);
		this.executor = executor;
		this.catalogName = catalogName;
		this.databaseName = schemaName;
		this.functionName = functionName;
	}

	@Override
	public void runInternal() throws HiveSQLException {
		Optional<Catalog> catalogOpt = executor.getTableEnvironment().getCatalog(catalogName);
		if (!catalogOpt.isPresent()) {
			throw new HiveSQLException(String.format("Catalog %s not found", catalogName));
		}
		try {
			// TODO: 1. We should also get built-in functions 2. function name might be just a pattern, and we need to
			// find all the matching functions.
			CatalogFunction function = catalogOpt.get().getFunction(new ObjectPath(databaseName, functionName));
			Object[] rowData = {
				catalogName,
				databaseName,
				functionName,
				function.getDescription(),
				DatabaseMetaData.functionNoTable, // TODO: return DatabaseMetaData.functionReturnsTable if it's UDTF
				function.getClassName()
			};
			// TODO: add rowData to rowSet.
			// rowSet.addRow(rowData);
			setState(OperationState.FINISHED);
		} catch (FunctionNotExistException ex) {
			throw new HiveSQLException(String.format("Function %s not found", functionName), ex);
		}
	}

}
