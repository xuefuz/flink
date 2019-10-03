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

import java.lang.reflect.Field;

/**
 * ReflectionUtils.
 */
public class ReflectionUtils {

	public static void setSuperField(Object object, int level, String fieldName, Object fieldValue) {
		Class<?> superClass = object.getClass();
		for (int i = 0; i < level; i++){
			superClass = superClass.getSuperclass();
		}

		try {
			Field field = superClass.getDeclaredField(fieldName);
			field.setAccessible(true);
			field.set(object, fieldValue);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException(String.format("Failed to set value for field %s in class %s", fieldName, superClass), e);
		}
	}

	public static Object getSuperField(Object object, int level, String fieldName) {
		Class<?> superClass = object.getClass();
		for (int i = 0; i < level; i++){
			superClass = superClass.getSuperclass();
		}

		try {
			Field field = superClass.getDeclaredField(fieldName);
			field.setAccessible(true);
			return field.get(object);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException(String.format("Failed to get value for field %s in class %s", fieldName, superClass), e);
		}
	}

//	public static void callSuperMethod(Object object, int level, String methodName, Class<?>[] signature, Object[] args) {
//		Class<?> superClass = object.getClass();
//		for (int i = 0; i < level; i++){
//			superClass = superClass.getSuperclass();
//		}
//
//		try {
//			Method method = superClass.getDeclaredMethod(methodName, signature);
//			method.setAccessible(true);
//			return method.invoke(object, args);
//		} catch(NoSuchFieldException | IllegalAccessException e) {
//			throw new RuntimeException(String.format("Failed to set value for field %s in class %s", fieldName, superClass), e);
//		}
//	}

}
