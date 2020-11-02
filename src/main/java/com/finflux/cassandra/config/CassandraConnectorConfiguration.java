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
package com.finflux.cassandra.config;

import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.finflux.cassandra.core.CassandraConfigDetails;
import com.finflux.cassandra.core.CassandraConnectionData;
import com.finflux.cassandra.core.CassandraJourney;
import com.finflux.cassandra.core.CassandraJourneyFactory;
import com.finflux.cassandra.core.CassandraJourneyRoute;
import com.finflux.cassandra.core.CassandraSessionBuilderFactory;
import com.finflux.cassandra.core.CassandraSessionProvider;

@Configuration
public class CassandraConnectorConfiguration {

    private final CassandraConfigDetails cassandraConfigDetails;

    public CassandraConnectorConfiguration(final CassandraConfigDetails cassandraConfigDetails) {
        this.cassandraConfigDetails = cassandraConfigDetails;
    }

    @Bean
    public CassandraSessionProvider cassandraSessionProvider(final CassandraJourneyFactory cassandraJourneyFactory) {
        final CassandraSessionBuilderFactory factory = new CassandraSessionBuilderFactory();
        final CassandraSessionProvider cassandraSessionProvider = new CassandraSessionProvider(
                factory.buildFactory(this.cassandraConfigDetails));
        final List<CassandraConnectionData> connectionDetails = this.cassandraConfigDetails.getTenantConnectionDetails();
        final List<CassandraJourneyRoute> cassandraJourneyRoutes = this.cassandraConfigDetails.getMigrationScripts();
        for (final CassandraConnectionData cassandraConnectionData : connectionDetails) {
            cassandraSessionProvider.getSession(cassandraConnectionData);
            if (!cassandraJourneyRoutes.isEmpty()) {
                final CassandraJourney cassandraJourney = cassandraJourneyFactory.create(cassandraSessionProvider, cassandraConnectionData);
                for (final CassandraJourneyRoute cassandraJourneyRoute : cassandraJourneyRoutes) {
                    cassandraJourney.start(cassandraJourneyRoute, cassandraConnectionData.getKeySpaceIdentifier());
                }
                cassandraJourney.cleanUp();
            }
        }
        return cassandraSessionProvider;
    }

    @Bean
    public CassandraJourneyFactory cassandraJourneyFactory() {
        return new CassandraJourneyFactory(this.cassandraConfigDetails.getApplicationName());
    }
}
