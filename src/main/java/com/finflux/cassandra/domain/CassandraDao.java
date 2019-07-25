package com.finflux.cassandra.domain;

import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Update;

public interface CassandraDao<T> {

    @Insert
    void save(T entity);

    @Delete
    void delete(T entity);

    @Update
    void update(T entity);
}
