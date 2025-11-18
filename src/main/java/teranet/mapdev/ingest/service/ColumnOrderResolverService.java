package teranet.mapdev.ingest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for resolving column order from database table schema
 * 
 * This service queries information_schema.columns to get the ordered
 * list of columns for a given table, which is essential when loading
 * data without headers (like TSV files from Polaris).
 * 
 * Column order information is cached per table for performance.
 */
@Service
public class ColumnOrderResolverService {

    private static final Logger log = LoggerFactory.getLogger(ColumnOrderResolverService.class);

    private final DataSource dataSource;

    // Cache: "schema.table" -> List of column names in ordinal order
    private final Map<String, List<String>> columnOrderCache = new ConcurrentHashMap<>();

    @Value("${spring.datasource.schema}")
    private String schema;

    public ColumnOrderResolverService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Get ordered list of columns for a table
     * 
     * @param schema    Schema name
     * @param tableName Table name
     * @return Ordered list of column names
     * @throws SQLException if table doesn't exist or query fails
     */
    public List<String> getColumnOrder(String tableName) throws SQLException {
        String cacheKey = tableName;

        // Check cache first
        if (columnOrderCache.containsKey(cacheKey)) {
            log.debug("Returning cached column order for {}", cacheKey);
            return columnOrderCache.get(cacheKey);
        }

        // Query database for column information
        List<String> columns = queryColumnOrder(tableName);

        // Cache the result
        columnOrderCache.put(cacheKey, columns);

        log.info("Resolved and cached column order for {}: {} columns", cacheKey, columns.size());

        return columns;
    }

    /**
     * Query table directly to get column order
     */
    private List<String> queryColumnOrder(String tableName) throws SQLException {
        String sql = "SELECT * FROM " + schema + "." + tableName + " LIMIT 0";

        List<String> columns = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                columns.add(metaData.getColumnName(i));
            }
        }

        if (columns.isEmpty()) {
            throw new SQLException(
                    String.format("Table %s.%s does not exist or has no columns", schema, tableName));
        }

        return columns;
    }

    /**
     * Check if a table exists in the database
     */
    public boolean tableExists(String tableName) {
        try {
            getColumnOrder(tableName);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Get column count for a table
     */
    public int getColumnCount(String tableName) throws SQLException {
        return getColumnOrder(tableName).size();
    }

    /**
     * Clear the cache (useful for testing or schema changes)
     */
    public void clearCache() {
        columnOrderCache.clear();
        log.info("Column order cache cleared");
    }

    /**
     * Clear cache for a specific table
     */
    public void clearCache(String schema, String tableName) {
        String cacheKey = schema + "." + tableName;
        columnOrderCache.remove(cacheKey);
        log.debug("Cache cleared for {}", cacheKey);
    }
}
