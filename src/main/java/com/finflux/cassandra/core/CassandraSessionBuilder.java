package com.finflux.cassandra.core;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;

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
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.finflux.cassandra.codec.LocalDateTimeCodec;
import com.finflux.cassandra.util.ContactPointUtils;

import software.aws.mcs.auth.SigV4AuthProvider;

public interface CassandraSessionBuilder {

    default DriverConfigLoader buildDriverConfigLoader(final CassandraConnectionData cassandraConnectionData) {
        final ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder = DriverConfigLoader.programmaticBuilder();
        driverConfigLoaderBuilder.withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class);
        driverConfigLoaderBuilder.withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "develop-at-963084729315");
        driverConfigLoaderBuilder.withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "1s2kZSKN1GpyeJmrl1YspWLVQSHsHqqW3yqyds41LIY=");
        driverConfigLoaderBuilder.withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM");
        driverConfigLoaderBuilder.withString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY,"LOCAL_QUORUM");
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
        builder.withLocalDatacenter("ap-south-1");
        builder.addTypeCodecs(new LocalDateTimeCodec());
        try {
			builder.withSslContext(SSLContext.getDefault());
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
        //builder.withAuthProvider(new SigV4AuthProvider("ap-south-1"));
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
