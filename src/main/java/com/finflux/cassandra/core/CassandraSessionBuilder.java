package com.finflux.cassandra.core;

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
import com.finflux.cassandra.codec.LocalDateTimeCodec;
import com.finflux.cassandra.util.ContactPointUtils;

public interface CassandraSessionBuilder {

    default DriverConfigLoader buildDriverConfigLoader(final CassandraConnectionData cassandraConnectionData) {
        final ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder = DriverConfigLoader.programmaticBuilder();
        driverConfigLoaderBuilder.withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class);
        driverConfigLoaderBuilder.withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, cassandraConnectionData.getUserName());
        driverConfigLoaderBuilder.withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, cassandraConnectionData.getPassword());
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
        final DriverConfigLoader driverConfigLoader = driverConfigLoaderBuilder.build();
        return driverConfigLoader;
    }

    default CqlSession buildSession(final CassandraConnectionData cassandraConnectionData) {
        final CqlSessionBuilder builder = getSessionBuilder(cassandraConnectionData);
        builder.withKeyspace(CqlIdentifier.fromInternal(cassandraConnectionData.getKeySpaceIdentifier()));
        return builder.build();
    }

    default CqlSessionBuilder getSessionBuilder(final CassandraConnectionData cassandraConnectionData) {
        final DriverConfigLoader driverConfigLoader = buildDriverConfigLoader(cassandraConnectionData);
        final CqlSessionBuilder builder = CqlSession.builder();
        ContactPointUtils.process(builder, cassandraConnectionData.getContactPoints());
        builder.withConfigLoader(driverConfigLoader);
        builder.withLocalDatacenter(cassandraConnectionData.getDataCenterName());
        builder.addTypeCodecs(new LocalDateTimeCodec());
        return builder;
    }

    default CqlSession buildSessionWithoutKeySpace(@Nonnull final CassandraConnectionData cassandraConnectionData) {
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

    public CqlSession createSession(CassandraConnectionData cassandraConnectionData);

}
