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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.AbstractService;
import org.apache.hive.service.Service;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * FlinkCLIService.
 */
public class FlinkCLIService extends CLIService {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkCLIService.class);

	private HiveServer2 hiveServer2;
	private Executor executor;

	public FlinkCLIService(HiveServer2 hiveServer2, Executor executor) {
		super(hiveServer2);
		this.hiveServer2 = hiveServer2;
		this.executor = executor;
	}

	@Override
	public synchronized void init(HiveConf hiveConf) {
		// Set upper class filed hiveConf
		ReflectionUtils.setSuperField(this, 3, "hiveConf", hiveConf);

		FlinkSessionManager flinkSessionManager = new FlinkSessionManager(this.hiveServer2, this.executor);
		// Replace parent's sessionManager with our own FlinkSessionManager instance.
		ReflectionUtils.setSuperField(this, 1, "sessionManager", flinkSessionManager);
		addService(flinkSessionManager);

		//  If the hadoop cluster is secure, do a kerberos login for the service from the keytab
		if (UserGroupInformation.isSecurityEnabled()) {
			try {
				HiveAuthFactory.loginFromKeytab(hiveConf);
				UserGroupInformation flinkServiceUGI = Utils.getUGI();
			} catch (IOException e) {
				throw new ServiceException("Unable to login to kerberos with given principal/keytab", e);
			} catch (LoginException e) {
				throw new ServiceException("Unable to login to kerberos with given principal/keytab", e);
			}
			// Set service UGI

			// Also try creating a UGI object for the SPNego principal
			String principal = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_SPNEGO_PRINCIPAL);
			String keyTabFile = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_SPNEGO_KEYTAB);
			if (principal.isEmpty() || keyTabFile.isEmpty()) {
				LOG.info("SPNego httpUGI not created, spNegoPrincipal: " + principal +
					", ketabFile: " + keyTabFile);
			} else {
				try {
					UserGroupInformation httpUGI = HiveAuthFactory.loginFromSpnegoKeytabAndReturnUGI(hiveConf);
					// set httpUGI field
					LOG.info("SPNego httpUGI successfully created.");
				} catch (IOException e) {
					LOG.warn("SPNego httpUGI creation failed: ", e);
				}
			}
		}

		// creates connection to HMS and thus *must* occur after kerberos login above
		try {
			// call parent's private method
			applyAuthorizationConfigPolicy(hiveConf);
		} catch (Exception e) {
			throw new RuntimeException("Error applying authorization policy on hive configuration: "
				+ e.getMessage(), e);
		}

		initCompositeService(hiveConf);
	}

	private void initCompositeService(HiveConf hiveConf) {
		// initCompositeService(hiveConf);
		List<Service> serviceList = (List<Service>) ReflectionUtils.getSuperField(this, 2, "serviceList");
		for (Service service : serviceList) {
			service.init(hiveConf);
		}
		try {
			Method method = AbstractService.class.getDeclaredMethod("ensureCurrentState", STATE.class);
			method.setAccessible(true);
			method.invoke(this, STATE.NOTINITED);
			ReflectionUtils.setSuperField(this, 3, "hiveConf", hiveConf);
			method = AbstractService.class.getDeclaredMethod("changeState", STATE.class);
			method.setAccessible(true);
			method.invoke(this, STATE.INITED);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Copied from Hive.
	 * @param newHiveConf
	 * @throws HiveException
	 * @throws MetaException
	 */
	private void applyAuthorizationConfigPolicy(HiveConf newHiveConf) throws HiveException, MetaException {
		// authorization setup using SessionState should be revisited eventually, as
		// authorization and authentication are not session specific settings
		SessionState ss = new SessionState(newHiveConf);
		ss.setIsHiveServerQuery(true);
		SessionState.start(ss);
		ss.applyAuthorizationPolicy();
	}

	@Override
	public GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType getInfoType) throws HiveSQLException {
		switch(getInfoType) {
			case CLI_SERVER_NAME:
				return new GetInfoValue("Flink");
			case CLI_DBMS_NAME:
				return new GetInfoValue("Flink SQL");
			case CLI_DBMS_VER:
				return new GetInfoValue("1.10.0"); // TODO: get version from somewhere
			default:
				return super.getInfo(sessionHandle, getInfoType);
		}
	}

}
