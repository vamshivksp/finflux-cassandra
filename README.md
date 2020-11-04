# Apache Cassandra Helper

This project provides helps for using Cassandra in services.

## Build command
  - Publish to local maven repository
   ./gradlew clean build publishToMavenLocal
  - Publish to s3 maven repository
   ./gradlew clean build publish

## Prerequisites
### Runtime
Install Java 8 as described at https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html.

### Installation
Install Apache Cassandra as described at https://wiki.apache.org/cassandra/GettingStarted.

#### Usage
This jar requires a implementation of CassandraConfigDetails from application 

## Multi-tenancy
Multi-tenancy is reached by providing separate key space on a per tenant basis.

For every tenant a new keyspace is created internally. can be passed using CassandraConnectionData. if you are using same connector details for  keyspaces then reuse the same session

## Versioning
The version numbers follow the [Semantic Versioning](http://semver.org/) scheme.

In addition to MAJOR.MINOR.PATCH the following postfixes are used to indicate the development state.

* RELEASE - _General availability_ indicates that this release is the best available version and is recommended for all usage.

The versioning layout is {MAJOR}.{MINOR}.{PATCH}-{INDICATOR}[.{PATCH}]. Only milestones and release candidates can  have patch versions. Some examples:

0.1.0-RELEASE

# Migrations

Use CassandraJourneyFactory to manage migrations within a key space.

