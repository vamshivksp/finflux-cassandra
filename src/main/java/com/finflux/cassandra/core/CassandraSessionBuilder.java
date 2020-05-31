package com.finflux.cassandra.core;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;

import javax.annotation.Nonnull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;

public interface CassandraSessionBuilder {
    
    default ProgrammaticDriverConfigLoaderBuilder getDriverConfigLoader(final CassandraConnectionData cassandraConnectionData) {
        final ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder = DriverConfigLoader.programmaticBuilder();
        driverConfigLoaderBuilder.withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class);
        driverConfigLoaderBuilder.withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "develop-at-963084729315");
        driverConfigLoaderBuilder.withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "1s2kZSKN1GpyeJmrl1YspWLVQSHsHqqW3yqyds41LIY=");
        driverConfigLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, cassandraConnectionData.getLocalPoolSize());
        driverConfigLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, cassandraConnectionData.getRemotePoolSize());
        driverConfigLoaderBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
                Duration.ofSeconds(cassandraConnectionData.getRequestTimeOut()));
        driverConfigLoaderBuilder.withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT,
                Duration.ofSeconds(cassandraConnectionData.getRequestTimeOut()));
        driverConfigLoaderBuilder.withDuration(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT,
                Duration.ofSeconds(cassandraConnectionData.getRequestTimeOut()));
        driverConfigLoaderBuilder.withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT,
                Duration.ofSeconds(cassandraConnectionData.getRequestTimeOut()));
        driverConfigLoaderBuilder.withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT,
                Duration.ofSeconds(cassandraConnectionData.getRequestTimeOut()));
        driverConfigLoaderBuilder.withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT,
                Duration.ofSeconds(cassandraConnectionData.getRequestTimeOut()));
        driverConfigLoaderBuilder.withDuration(DefaultDriverOption.REPREPARE_TIMEOUT,
                Duration.ofSeconds(cassandraConnectionData.getRequestTimeOut()));
        return driverConfigLoaderBuilder;
    }

    default CqlSession buildSession(final CassandraConnectionData cassandraConnectionData) throws NoSuchAlgorithmException {
        final CqlSessionBuilder builder = getSessionBuilder(cassandraConnectionData);
        builder.withKeyspace(CqlIdentifier.fromInternal(cassandraConnectionData.getKeySpaceIdentifier()));
        return builder.build();
    }

    default CqlSessionBuilder getSessionBuilder(final CassandraConnectionData cassandraConnectionData) throws NoSuchAlgorithmException {
        final ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder = getDriverConfigLoader(cassandraConnectionData);
        final CassandraProvider cassandraProvider = CassandraProvider.getProvider("AWS_MCS");
        return cassandraProvider.getSessionBuilder(cassandraConnectionData, driverConfigLoaderBuilder);
    }

    default CqlSession buildSessionWithoutKeySpace(@Nonnull final CassandraConnectionData cassandraConnectionData)
            throws NoSuchAlgorithmException {
        final CqlSessionBuilder builder = getSessionBuilder(cassandraConnectionData);
        return builder.build();
    }

    default void createKeySpace(final CassandraConnectionData cassandraConnectionData, final CqlSession session) {
        final CreateKeyspaceStart createKeyspaceStart = SchemaBuilder
                .createKeyspace(CqlIdentifier.fromInternal(cassandraConnectionData.getKeySpaceIdentifier())).ifNotExists();
        final CreateKeyspace createKeyspace = ReplicationStrategyResolver.replicationStrategy(cassandraConnectionData.getReplicationType(),
                cassandraConnectionData.getReplicationDetails(), createKeyspaceStart);
        session.execute(createKeyspace.build());
    }

    public CqlSession createSession(CassandraConnectionData cassandraConnectionData)  throws NoSuchAlgorithmException;
    
}
