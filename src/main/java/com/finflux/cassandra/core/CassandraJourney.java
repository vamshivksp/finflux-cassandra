/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.finflux.cassandra.core;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;

public class CassandraJourney {

    private final Logger logger = LoggerFactory.getLogger(CassandraJourney.class);
    private final String applicationName;
    private final CqlSession session;
    private final String schemaTableName;

    public CassandraJourney(final String applicationName, final CqlSession session) {
        super();
        this.applicationName = applicationName == null ? "" : applicationName;
        this.session = session;
        this.schemaTableName = this.applicationName + "_cassandra_schema_table";
    }

    public void start(final CassandraJourneyRoute cassandraJourneyRoute) {
        // check for version
        final Select query = selectFrom(this.schemaTableName).column("hash_value").whereColumn("version").isEqualTo(bindMarker());
        final PreparedStatement preparedSelect = this.session.prepare(query.build());
        try {
            final ResultSet resultSet = this.session.execute(preparedSelect.bind(cassandraJourneyRoute.getVersion()));
        } catch (InvalidQueryException e) {
            // TODO: handle exception
        }
        final ResultSet resultSet = this.session.execute(preparedSelect.bind(cassandraJourneyRoute.getVersion()));
        final Row row = resultSet.one();
        if (Objects.nonNull(row)) {
            final Integer fetchedHashValue = row.get("hash_value", Integer.class);
            if (!Objects.equals(fetchedHashValue, cassandraJourneyRoute.getHashValue())) {
                throw new IllegalStateException("Version mismatch for " + cassandraJourneyRoute.getVersion());
            }
        } else {
            cassandraJourneyRoute.getCassandraJourneyWaypoints().forEach(waypoint -> this.session.execute(waypoint.getStatement()));
            final RegularInsert insertStatement = insertInto(this.schemaTableName)
                    .value("version", literal(cassandraJourneyRoute.getVersion()))
                    .value("hash_value", literal(cassandraJourneyRoute.getHashValue()));
            final SimpleStatement statement = insertStatement.build();
            this.session.execute(statement);
        }
    }

    void init() {
        try {
            this.session.execute(SchemaBuilder.createTable(CqlIdentifier.fromInternal(this.schemaTableName)).ifNotExists()
                    .withPartitionKey("version", DataTypes.TEXT).withColumn("hash_value", DataTypes.INT).build());
        } catch (final Throwable th) {
            this.logger.warn("Schema table for '{}' already exists.", this.schemaTableName);
        }
    }
    
    public void cleanUp() {
        this.logger.info("Clean up Session");
        this.session.close();
    }
}
