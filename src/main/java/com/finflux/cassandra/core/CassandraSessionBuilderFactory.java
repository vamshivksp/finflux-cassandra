package com.finflux.cassandra.core;

public class CassandraSessionBuilderFactory {

    public CassandraSessionBuilder buildFactory(final CassandraConfigDetails cassandraConfigDetails) {

        CassandraSessionBuilder cassandraSessionBuilder = null;
        if (cassandraConfigDetails.bindKeySpaceToSession()) {
            cassandraSessionBuilder = new CassandraSessionWithKeySpace();
        } else {
            cassandraSessionBuilder = new CassandraSessionWithoutKeySpace();
        }

        return cassandraSessionBuilder;

    }

}
