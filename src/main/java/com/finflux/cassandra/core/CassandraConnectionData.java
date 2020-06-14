package com.finflux.cassandra.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@Builder
@NoArgsConstructor
public class CassandraConnectionData {

    @Default
    private String sessionIdentifier = "default";
    @Default
    private String keySpaceIdentifier = "default";
    @Default
    private String userName = "cassandra";
    @Default
    private String password = "cassandra";
    @Default
    private String contactPoints = "localhost:9042";
    @Default
    private String replicationType = "Simple";
    @Default
    private String replicationDetails = "1";
    @Default
    private String dataCenterName = "datacenter1";
    @Default
    private Integer localPoolSize = 1;
    @Default
    private Integer remotePoolSize = 1;
    @Default
    private Integer requestTimeOut = 2;
    @Default
    private String provider = "STANDALONE";

}
