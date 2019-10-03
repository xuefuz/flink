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
import org.apache.flink.table.server.gateway.Executor;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.operation.GetSchemasOperation;
import org.apache.hive.service.cli.session.HiveSession;

import java.util.List;
import java.util.Optional;

/**
 * Basically getDatabases(String catalogName, String pattern).
 */
public class FlinkGetSchemasOperation extends GetSchemasOperation {
	private Executor executor;
	private String catalogName;
	private String schemaName;

	public FlinkGetSchemasOperation(Executor executor, HiveSession parentSession, String catalogName, String schemaName) {
		super(parentSession, catalogName, schemaName);
		this.executor = executor;
		this.catalogName = catalogName;
		this.schemaName = schemaName;
	}

	@Override
	public void runInternal() throws HiveSQLException {
		Optional<Catalog> catalogOpt = executor.getTableEnvironment().getCatalog(catalogName);
		if (!catalogOpt.isPresent()) {
			throw new HiveSQLException(String.format("Catalog %s not found", catalogName));
		}

		List<String> databases = catalogOpt.get().listDatabases();
		RowSet rowSet = RowSetFactory.create(getResultSetSchema(), getProtocolVersion(), false);
		for (String db : databases) {
			rowSet.addRow(new Object[] {db, catalogName});
		}

		ReflectionUtils.setSuperField(this, 1, "rowSet", rowSet);
		setState(OperationState.FINISHED);
	}

}
