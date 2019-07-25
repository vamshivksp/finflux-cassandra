
package com.finflux.cassandra.codec;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.finflux.cassandra.util.DateConverter;

import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class LocalDateTimeCodec implements TypeCodec<LocalDateTime> {

    @Override
    public GenericType<LocalDateTime> getJavaType() {
        return GenericType.of(LocalDateTime.class);
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.TIMESTAMP;
    }

    @Override
    public ByteBuffer encode(final LocalDateTime value, final ProtocolVersion protocolVersion) {
        final Long epochMillis = DateConverter.toEpochMillis(value);
        return TypeCodecs.BIGINT.encode(epochMillis, protocolVersion);
    }

    @Override
    public LocalDateTime decode(final ByteBuffer bytes, final ProtocolVersion protocolVersion) {
        final Long epochMillis = TypeCodecs.BIGINT.decode(bytes, protocolVersion);
        return DateConverter.fromEpochMillis(epochMillis);
    }

    @Override
    public String format(final LocalDateTime value) {
        return (value == null) ? "NULL" : Strings.quote(DateConverter.toIsoString(value));
    }

    @Override
    public LocalDateTime parse(@Nullable String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) { return null; }

        // single quotes are optional for long literals, mandatory for date
        // patterns
        // strip enclosing single quotes, if any
        if (Strings.isQuoted(value)) {
            value = Strings.unquote(value);
        }

        if (Strings.isLongLiteral(value)) { return DateConverter.fromEpochMillis(Long.parseLong(value)); }
        return DateConverter.fromIsoString(value);

    }
}
