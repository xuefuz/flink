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
import org.apache.hive.service.AbstractService;
import org.apache.hive.service.Service;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.hive.service.server.HiveServer2;

import java.lang.reflect.Method;
import java.util.List;


/**
 * FlinkThriftServer.
 */
public class FlinkThriftServer extends HiveServer2 {
	private Executor executor;

	public FlinkThriftServer(Executor executor) {
		super();
		this.executor = executor;
	}

	@Override
	public void init(HiveConf hiveConf) {
		FlinkCLIService flinkCliService = new FlinkCLIService(this, executor);
		// Set flinkCliSerivice for parent
		ReflectionUtils.setSuperField(this, 1, "cliService", flinkCliService);
		addService(flinkCliService);

		final HiveServer2 hiveServer2 = this;
		Runnable oomHook = new Runnable() {
			@Override
			public void run() {
				hiveServer2.stop();
			}
		};

		ThriftCLIService thriftCLIService;
		if (isHTTPTransportMode(hiveConf)) {
			thriftCLIService = new ThriftHttpCLIService(flinkCliService, oomHook);
		} else {
			thriftCLIService = new ThriftBinaryCLIService(flinkCliService, oomHook);
		}
		ReflectionUtils.setSuperField(this, 1, "thriftCLIService", thriftCLIService);
		addService(thriftCLIService);

		initCompositeService(hiveConf);
	}

	private void initCompositeService(HiveConf hiveConf) {
		// initCompositeService(hiveConf);
		List<Service> serviceList = (List<Service>) ReflectionUtils.getSuperField(this, 2, "serviceList");
		for (Service service : serviceList) {
			service.init(hiveConf);
		}
		try {
//			Method[] methods = AbstractService.class.getDeclaredMethods();
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

}
