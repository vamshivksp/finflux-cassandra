package com.finflux.cassandra.core;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.finflux.cassandra.codec.LocalDateTimeCodec;
import com.finflux.cassandra.util.ContactPointUtils;

public enum CassandraProvider {

    STANDALONE() {

        @Override
        public CqlSessionBuilder getSessionBuilder(final CassandraConnectionData cassandraConnectionData,
                final ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder) {
            final CqlSessionBuilder builder = CqlSession.builder();
            ContactPointUtils.process(builder, cassandraConnectionData.getContactPoints());
            builder.withConfigLoader(driverConfigLoaderBuilder.build());
            builder.addTypeCodecs(new LocalDateTimeCodec());
            return builder;
        }
    },

    AWS_MCS() {

        @Override
        public CqlSessionBuilder getSessionBuilder(final CassandraConnectionData cassandraConnectionData,
                final ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder) throws NoSuchAlgorithmException {
            driverConfigLoaderBuilder.withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM");
            final CqlSessionBuilder builder = STANDALONE.getSessionBuilder(cassandraConnectionData, driverConfigLoaderBuilder);
            builder.withLocalDatacenter("ap-south-1");
            builder.withSslContext(SSLContext.getDefault());
            return builder;
        }
    };

    public abstract CqlSessionBuilder getSessionBuilder(final CassandraConnectionData cassandraConnectionData,
            final ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder) throws NoSuchAlgorithmException;

    private static final Map<String, CassandraProvider> nameToEnumMap = new HashMap<>();
    static {
        for (final CassandraProvider provider : CassandraProvider.values()) {
            nameToEnumMap.put(provider.name(), provider);
        }
    }

    public static CassandraProvider getProvider(final String name) {
        final CassandraProvider provider = nameToEnumMap.get(name.toUpperCase());
        return provider;
    }
}
