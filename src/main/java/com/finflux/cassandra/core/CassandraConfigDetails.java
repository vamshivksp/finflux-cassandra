package com.finflux.cassandra.core;

import java.util.List;

public interface CassandraConfigDetails {

    public String getApplicationName();
    
    public List<CassandraConnectionData>  getTenantConnectionDetails();
    
    public List<CassandraJourneyRoute>  getMigrationScripts();
    
    default boolean bindKeySpaceToSession() {
        return false;
    }
    
    
}
