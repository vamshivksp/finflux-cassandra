
package com.finflux.cassandra.core;

import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlSession;

public class CassandraSessionProvider {

    private final Logger logger = LoggerFactory.getLogger(CassandraSessionProvider.class);
    private final ConcurrentHashMap<String, CqlSession> sessionCache;
    private final CassandraSessionBuilder sessionBuilder;

    public CassandraSessionProvider(final CassandraSessionBuilder sessionBuilder) {
        this.sessionCache = new ConcurrentHashMap<>();
        this.sessionBuilder = sessionBuilder;
    }

    @Nonnull
    public CqlSession getSession(@Nonnull final CassandraConnectionData cassandraConnectionData) throws NoSuchAlgorithmException {
        Assert.notNull(cassandraConnectionData, "At least one contact point must be given.");
        try {
            CqlSession session = getTenantSession(cassandraConnectionData.getSessionIdentifier());
            this.sessionBuilder.createKeySpace(cassandraConnectionData, session);
            return session;
        } catch (final KeyspaceNotFoundException e) {
            final CqlSession session = this.sessionBuilder.createSession(cassandraConnectionData);
            if (Objects.nonNull(session)) {
                this.sessionCache.put(cassandraConnectionData.getSessionIdentifier(), session);
            }
            return session;
        }
    }
    
    @Nonnull
    public CqlSession getSessionForSchemaCreation(@Nonnull final CassandraConnectionData cassandraConnectionData) throws NoSuchAlgorithmException {
        Assert.notNull(cassandraConnectionData, "At least one contact point must be given.");
            final CqlSession session = this.sessionBuilder.buildSession(cassandraConnectionData);
            return session;
    }

    @Nonnull
    public CqlSession getTenantSession(@Nonnull final String sessionIdentifier) {
        Assert.notNull(sessionIdentifier, "A tenant with keyspace must be given.");
        if (this.sessionCache.containsKey(sessionIdentifier)) { return this.sessionCache.get(sessionIdentifier); }
        throw new KeyspaceNotFoundException(sessionIdentifier);
    }

    @PreDestroy
    private void cleanUp() {
        this.logger.info("Clean up cluster connections.");

        this.sessionCache.values().forEach(CqlSession::close);
        this.sessionCache.clear();
    }
}
