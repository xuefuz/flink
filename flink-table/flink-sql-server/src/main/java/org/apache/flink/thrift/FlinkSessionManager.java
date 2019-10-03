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

import org.apache.flink.table.server.gateway.Executor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.server.HiveServer2;

import java.util.Map;

/**
 * FlinkSessionManager.
 */
public class FlinkSessionManager extends SessionManager {
	private Executor executor = null;

	private FlinkOperationManager flinkOperationManager = new FlinkOperationManager();

	public FlinkSessionManager(HiveServer2 hiveServer2, Executor executor) {
		super(hiveServer2);
		this.executor = executor;
	}

	@Override
	public void init(HiveConf hiveConf) {
		OperationManager operationManager = new FlinkOperationManager();
		// Replace operationManager with our own instance.
		ReflectionUtils.setSuperField(this, 1, "operationManager", flinkOperationManager);
		super.init(hiveConf);
	}

	@Override
	public SessionHandle openSession(TProtocolVersion protocol, String username, String password, String ipAddress,
									Map<String, String> sessionConf, boolean withImpersonation, String delegationToken)
		throws HiveSQLException {
		SessionHandle sessionHandle = super.openSession(protocol, username, password, ipAddress, sessionConf, withImpersonation,
			delegationToken);
		HiveSession session = super.getSession(sessionHandle);
		// TODO: anything session related in Flink
		// Create a Flink Session which includes a SessionContext and a TableEnvironment
		// Update current catalog/db based on info in sessionConf.
//		SessionContext sessionContext = new SessionContext("my session", null);
//		Executor executor = new LocalExecutor(null, );
		FlinkSession flinkSession = new FlinkSession(this.executor); // For now, use the default executor.
		flinkOperationManager.handleToSession.put(sessionHandle, flinkSession);
		return sessionHandle;
	}

	@Override
	public synchronized void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
		super.closeSession(sessionHandle);
		// TODO: anything related to Flink
	}

}
