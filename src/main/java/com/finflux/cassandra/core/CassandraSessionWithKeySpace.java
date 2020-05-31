package com.finflux.cassandra.core;

import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.InvalidKeyspaceException;

public class CassandraSessionWithKeySpace implements CassandraSessionBuilder {

    private final Logger logger = LoggerFactory.getLogger(CassandraSessionWithKeySpace.class);

    @Override
    public CqlSession createSession(final CassandraConnectionData cassandraConnectionData) throws NoSuchAlgorithmException {
        this.logger.info("Create new session for keyspace [" + cassandraConnectionData.getKeySpaceIdentifier() + "].");
        try {
            return buildSession(cassandraConnectionData);
        } catch (final InvalidKeyspaceException ignored) {
            final CqlSession session = buildSessionWithoutKeySpace(cassandraConnectionData);
            createKeySpace(cassandraConnectionData, session);
            return buildSession(cassandraConnectionData);
        }
    }

}
