/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.learn.flink.connector.jdbc.meta;

import org.apache.derby.jdbc.EmbeddedXADataSource;

import javax.sql.XADataSource;

/**
 * DerbyDbMetadata.
 */
public class MysqlDbMetadata implements DbMetadata {
	private final String dbName;
	private final String dbInitUrl;
	private final String url;

	public MysqlDbMetadata(String schemaName) {
		dbName = "test-airflow-mysql.c1i7fy6grpdg.us-west-2.rds.amazonaws.com:3306/" + schemaName;
		url = "jdbc:mysql://" + dbName;
		dbInitUrl = url;
	}


	public String getDbName() {
		return dbName;
	}

	@Override
	public String getInitUrl() {
		return dbInitUrl;
	}

	@Override
	public XADataSource buildXaDataSource() {
		EmbeddedXADataSource ds = new EmbeddedXADataSource();
		ds.setDatabaseName(dbName);
		return ds;
	}

	@Override
	public String getDriverClass() {
		return "org.apache.derby.jdbc.EmbeddedDriver";
	}

	@Override
	public String getUrl() {
		return url;
	}

	@Override
	public String getLoginAccount() {
		return 	"airflow_data_db";
	}

	@Override
	public String getLoginPwd() {
		return "s4kEhEngSQmbbk6x";
	}
}
