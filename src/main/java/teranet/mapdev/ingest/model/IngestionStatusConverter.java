package teranet.mapdev.ingest.model;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/**
 * JPA AttributeConverter to handle PostgreSQL custom enum type 'ingestion_status'.
 * This converter ensures proper type casting when inserting/updating enum values
 * from Java to PostgreSQL custom enum type.
 * 
 * The converter simply returns the enum name as a string, and PostgreSQL
 * will automatically cast it to the ingestion_status enum type based on
 * the columnDefinition in the entity.
 */
@Converter(autoApply = false)
public class IngestionStatusConverter implements AttributeConverter<IngestionManifest.Status, String> {
    
    /**
     * Convert Java enum to String (PostgreSQL will cast to enum type)
     * @param status Java enum value
     * @return Enum name as string
     */
    @Override
    public String convertToDatabaseColumn(IngestionManifest.Status status) {
        if (status == null) {
            return null;
        }
        return status.name();
    }
    
    /**
     * Convert String from database to Java enum
     * @param dbData Database value (String)
     * @return Java enum value
     */
    @Override
    public IngestionManifest.Status convertToEntityAttribute(String dbData) {
        if (dbData == null) {
            return null;
        }
        return IngestionManifest.Status.valueOf(dbData);
    }
}
