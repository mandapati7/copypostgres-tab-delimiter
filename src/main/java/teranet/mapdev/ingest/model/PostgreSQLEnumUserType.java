package teranet.mapdev.ingest.model;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Custom Hibernate UserType to handle PostgreSQL enum types.
 * This type explicitly casts string values to PostgreSQL enum type during SQL execution.
 */
public class PostgreSQLEnumUserType implements UserType<IngestionManifest.Status> {

    @Override
    public int getSqlType() {
        return Types.OTHER;  // PostgreSQL custom type
    }

    @Override
    public Class<IngestionManifest.Status> returnedClass() {
        return IngestionManifest.Status.class;
    }

    @Override
    public boolean equals(IngestionManifest.Status x, IngestionManifest.Status y) {
        return x == y;
    }

    @Override
    public int hashCode(IngestionManifest.Status x) {
        return x == null ? 0 : x.hashCode();
    }

    @Override
    public IngestionManifest.Status nullSafeGet(ResultSet rs, int position, 
                                                  SharedSessionContractImplementor session, 
                                                  Object owner) throws SQLException {
        String value = rs.getString(position);
        if (value == null) {
            return null;
        }
        return IngestionManifest.Status.valueOf(value);
    }

    @Override
    public void nullSafeSet(PreparedStatement st, IngestionManifest.Status value, 
                           int index, SharedSessionContractImplementor session) throws SQLException {
        if (value == null) {
            st.setNull(index, Types.OTHER);
        } else {
            // Set as OTHER type with explicit PostgreSQL type name
            st.setObject(index, value.name(), Types.OTHER);
        }
    }

    @Override
    public IngestionManifest.Status deepCopy(IngestionManifest.Status value) {
        return value;  // Enums are immutable
    }

    @Override
    public boolean isMutable() {
        return false;  // Enums are immutable
    }

    @Override
    public Serializable disassemble(IngestionManifest.Status value) {
        return value;
    }

    @Override
    public IngestionManifest.Status assemble(Serializable cached, Object owner) {
        return (IngestionManifest.Status) cached;
    }

    @Override
    public IngestionManifest.Status replace(IngestionManifest.Status detached, 
                                            IngestionManifest.Status managed, 
                                            Object owner) {
        return detached;
    }
}
