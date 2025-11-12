-- Example: Configure data transformation for IM2 files
-- This enables the IM2Transformer to convert dates and trim fields

-- First, check current configuration
SELECT file_pattern, enable_data_transformation, transformer_class_name 
FROM title_d_app.file_validation_rules 
WHERE file_pattern = 'IM2';

-- Enable transformation for IM2 files
UPDATE title_d_app.file_validation_rules 
SET enable_data_transformation = TRUE,
    transformer_class_name = 'teranet.mapdev.ingest.transformer.IM2Transformer'
WHERE file_pattern = 'IM2';

-- Disable transformation for PM3 files (if they don't need it)
UPDATE title_d_app.file_validation_rules 
SET enable_data_transformation = FALSE,
    transformer_class_name = NULL
WHERE file_pattern = 'PM3';

-- Verify configuration
SELECT 
    file_pattern,
    enable_data_transformation,
    transformer_class_name,
    validation_enabled,
    expected_tab_count
FROM title_d_app.file_validation_rules 
ORDER BY file_pattern;
