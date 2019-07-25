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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart;

public class ReplicationStrategyResolver {

    private ReplicationStrategyResolver() {
        super();
    }

    public static CreateKeyspace replicationStrategy(final String type, final String replicas,
            final CreateKeyspaceStart createKeyspaceStart) {
        if (type.equalsIgnoreCase("Simple")) {
            int numberOfReplicas = 1;
            if (Objects.nonNull(replicas)) {
                numberOfReplicas = Integer.parseInt(replicas);
            }
            return createKeyspaceStart.withSimpleStrategy(numberOfReplicas);
        } else if (type.equalsIgnoreCase("Network")) {
            final String[] splitReplicas = replicas.split(",");
            final Map<String, Integer> replications = new HashMap<>();
            for (int i = 0; i < splitReplicas.length; i++) {
                final String[] replicaDataCenter = splitReplicas[i].split(":");
                final String dataCenterName = replicaDataCenter[0].trim();
                int numberOfReplicas = 1;
                if (Objects.nonNull(replicaDataCenter[1])) {
                    numberOfReplicas = Integer.parseInt(replicaDataCenter[1].trim());
                }
                replications.put(dataCenterName, numberOfReplicas);
            }
            return createKeyspaceStart.withNetworkTopologyStrategy(replications);
        } else {
            throw new IllegalArgumentException("Unknown replication strategy: " + type);
        }
    }
}
