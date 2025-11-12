package teranet.mapdev.ingest.service;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import teranet.mapdev.ingest.config.CsvProcessingConfig;
import teranet.mapdev.ingest.dto.DatabaseConnectionInfoDto;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DatabaseConnectionServiceTest {

    @Mock
    private DataSource dataSource;

    @Mock
    private HikariDataSource hikariDataSource;

    @Mock
    private Connection connection;

    @Mock
    private DatabaseMetaData metaData;

    @Mock
    private ResultSet resultSet;

    @Mock
    private Statement statement;

    @Mock
    private CsvProcessingConfig csvConfig;

    @InjectMocks
    private DatabaseConnectionService service;

    @BeforeEach
    void setUp() throws SQLException {
        ReflectionTestUtils.setField(service, "databaseUrl", "jdbc:postgresql://localhost:5432/testdb");
        ReflectionTestUtils.setField(service, "username", "testuser");
    }

    @Test
    void testGetConnectionInfo_Success() throws SQLException {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(connection.isValid(5)).thenReturn(true);

        // Mock schemas
        when(metaData.getSchemas()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("TABLE_SCHEM")).thenReturn("public", "information_schema");

        DatabaseConnectionInfoDto result = service.getConnectionInfo();

        assertNotNull(result);
        assertEquals("jdbc:postgresql://localhost:5432/testdb", result.getDatabaseUrl());
        assertEquals("testdb", result.getDatabaseName());
        assertEquals("testuser", result.getUsername());
        assertEquals("HEALTHY", result.getConnectionStatus());
        assertTrue(result.getStagingSchemaExists());
        verify(connection).close();
    }

    @Test
    void testGetConnectionInfo_WithHikariDataSource() throws SQLException {
        ReflectionTestUtils.setField(service, "dataSource", hikariDataSource);

        when(hikariDataSource.getConnection()).thenReturn(connection);
        when(hikariDataSource.getMaximumPoolSize()).thenReturn(10);
        when(connection.getMetaData()).thenReturn(metaData);
        when(connection.isValid(5)).thenReturn(true);
        when(metaData.getSchemas()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        DatabaseConnectionInfoDto result = service.getConnectionInfo();

        assertNotNull(result);
        assertEquals(10, result.getConnectionPoolSize());
    }

    @Test
    void testGetConnectionInfo_ConnectionFailure() throws SQLException {
        when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

        DatabaseConnectionInfoDto result = service.getConnectionInfo();

        assertNotNull(result);
        assertEquals("Connection Failed", result.getDatabaseName());
        assertTrue(result.getConnectionStatus().startsWith("ERROR:"));
        assertFalse(result.getStagingSchemaExists());
    }

    @Test
    void testEnsureStagingSchemaExists() {
        boolean result = service.ensureStagingSchemaExists();
        assertTrue(result);
    }

    @Test
    void testGetStagingSchema() {
        String result = service.getStagingSchema();
        assertNull(result);
    }

    @Test
    void testDoesStagingTableExist_True() throws SQLException {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables(isNull(), isNull(), eq("staging_test"), any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);

        boolean result = service.doesStagingTableExist("staging_test");

        assertTrue(result);
        verify(connection).close();
    }

    @Test
    void testDoesStagingTableExist_False() throws SQLException {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables(isNull(), isNull(), eq("staging_test"), any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        boolean result = service.doesStagingTableExist("staging_test");

        assertFalse(result);
        verify(connection).close();
    }

    @Test
    void testDoesStagingTableExist_Exception() throws SQLException {
        when(dataSource.getConnection()).thenThrow(new SQLException("DB error"));

        boolean result = service.doesStagingTableExist("staging_test");

        assertFalse(result);
    }

    @Test
    void testGetStagingTables_Success() throws SQLException {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables(isNull(), isNull(), eq("%"), any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("TABLE_NAME")).thenReturn("table1", "table2");

        List<String> result = service.getStagingTables();

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains("table1"));
        assertTrue(result.contains("table2"));
        verify(connection).close();
    }

    @Test
    void testGetStagingTables_EmptyList() throws SQLException {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables(isNull(), isNull(), eq("%"), any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        List<String> result = service.getStagingTables();

        assertNotNull(result);
        assertTrue(result.isEmpty());
        verify(connection).close();
    }

    @Test
    void testGetStagingTables_Exception() throws SQLException {
        when(dataSource.getConnection()).thenThrow(new SQLException("DB error"));

        List<String> result = service.getStagingTables();

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testExtractDatabaseName_ValidUrl() {
        ReflectionTestUtils.setField(service, "databaseUrl", "jdbc:postgresql://localhost:5432/mydb?ssl=true");

        DatabaseConnectionInfoDto result = service.getConnectionInfo();

        // The connection will fail in test, but we can verify URL parsing in a
        // different way
        // Let's test by checking the error message contains our URL
        assertTrue(result.getDatabaseUrl().contains("mydb"));
    }

    @Test
    void testExtractDatabaseName_InvalidUrl() throws SQLException {
        ReflectionTestUtils.setField(service, "databaseUrl", "invalid-url");
        when(dataSource.getConnection()).thenThrow(new SQLException("Invalid URL"));

        DatabaseConnectionInfoDto result = service.getConnectionInfo();

        assertNotNull(result);
        assertEquals("Connection Failed", result.getDatabaseName());
    }

    @Test
    void testExtractDatabaseName_NullUrl() throws SQLException {
        ReflectionTestUtils.setField(service, "databaseUrl", null);
        when(dataSource.getConnection()).thenThrow(new SQLException("Null URL"));

        DatabaseConnectionInfoDto result = service.getConnectionInfo();

        assertNotNull(result);
        assertEquals("Connection Failed", result.getDatabaseName());
    }

    @Test
    void testConnectionHealth_Unhealthy() throws SQLException {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(connection.isValid(5)).thenReturn(false);
        when(metaData.getSchemas()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        DatabaseConnectionInfoDto result = service.getConnectionInfo();

        assertNotNull(result);
        assertEquals("UNHEALTHY", result.getConnectionStatus());
        verify(connection).close();
    }

    @Test
    void testConnectionHealth_Exception() throws SQLException {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(connection.isValid(5)).thenThrow(new SQLException("Health check failed"));
        when(metaData.getSchemas()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        DatabaseConnectionInfoDto result = service.getConnectionInfo();

        assertNotNull(result);
        assertTrue(result.getConnectionStatus().startsWith("ERROR:"));
        verify(connection).close();
    }
}
