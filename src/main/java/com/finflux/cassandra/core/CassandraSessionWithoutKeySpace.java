package com.finflux.cassandra.core;

import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;

public class CassandraSessionWithoutKeySpace implements CassandraSessionBuilder {

    private final Logger logger = LoggerFactory.getLogger(CassandraSessionWithoutKeySpace.class);

    @Override
    public CqlSession createSession(final CassandraConnectionData cassandraConnectionData) throws NoSuchAlgorithmException {

        final CqlSession session = buildSessionWithoutKeySpace(cassandraConnectionData);
        createKeySpace(cassandraConnectionData, session);
        return session;
    }

}
