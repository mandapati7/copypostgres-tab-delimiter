-- Add data transformation configuration columns to file_validation_rules table
-- This allows configurable, per-file-pattern custom data transformations

ALTER TABLE title_d_app.file_validation_rules 
ADD COLUMN enable_data_transformation BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE title_d_app.file_validation_rules 
ADD COLUMN transformer_class_name VARCHAR(255);

-- Add comments for documentation
COMMENT ON COLUMN title_d_app.file_validation_rules.enable_data_transformation IS 'Whether to apply custom data transformations for this file pattern';
COMMENT ON COLUMN title_d_app.file_validation_rules.transformer_class_name IS 'Fully qualified Java class name implementing DataTransformer interface (e.g., teranet.mapdev.ingest.transformer.IM2Transformer). Leave NULL or blank to disable transformation.';

-- Example: Enable transformation for IM2 files
-- UPDATE file_validation_rules 
-- SET enable_data_transformation = TRUE,
--     transformer_class_name = 'teranet.mapdev.ingest.transformer.IM2Transformer'
-- WHERE file_pattern = 'IM2';
