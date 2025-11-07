package teranet.mapdev.ingest.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
@Slf4j
public class ColumnOrderResolverService {
    
    private final DataSource dataSource;
    
    // Cache: "schema.table" -> List of column names in ordinal order
    private final Map<String, List<String>> columnOrderCache = new ConcurrentHashMap<>();
    
    public ColumnOrderResolverService(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    
    /**
     * Get ordered list of columns for a table
     * 
     * @param schema Schema name
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
     * Query information_schema for column order
     */
    private List<String> queryColumnOrder(String tableName) throws SQLException {
        String sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
            ORDER BY ordinal_position
            """;
        
        List<String> columns = new ArrayList<>();
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, tableName.toLowerCase()); // PostgreSQL stores table names in lowercase

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString("column_name"));
                }
            }
        }
        
        if (columns.isEmpty()) {
            throw new SQLException(
                String.format("Table %s does not exist or has no columns", tableName)
            );
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
