package teranet.mapdev.ingest.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

/**
 * DTO for displaying database connection information
 * Used to show users the current database connection details
 */
@Data
@NoArgsConstructor
public class DatabaseConnectionInfoDto {
    
    @JsonProperty("database_url")
    private String databaseUrl;
    
    @JsonProperty("database_name") 
    private String databaseName;
    
    @JsonProperty("username")
    private String username;
    
    @JsonProperty("connection_pool_size")
    private Integer connectionPoolSize;
    
    @JsonProperty("connection_status")
    private String connectionStatus;
    
    @JsonProperty("schemas_available")
    private List<String> schemasAvailable;
    
    @JsonProperty("staging_schema_exists")
    private Boolean stagingSchemaExists;
    
    @JsonProperty("server_time")
    private LocalDateTime serverTime;

    // Custom constructor
    public DatabaseConnectionInfoDto(String databaseUrl, String databaseName, String username, 
                                   Integer connectionPoolSize, String connectionStatus, 
                                   List<String> schemasAvailable, Boolean stagingSchemaExists) {
        this.databaseUrl = databaseUrl;
        this.databaseName = databaseName;
        this.username = username;
        this.connectionPoolSize = connectionPoolSize;
        this.connectionStatus = connectionStatus;
        this.schemasAvailable = schemasAvailable;
        this.stagingSchemaExists = stagingSchemaExists;
        this.serverTime = LocalDateTime.now();
    }
}