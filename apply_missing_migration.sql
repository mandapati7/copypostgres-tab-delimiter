-- =====================================================
-- Apply Missing Migrations V5 and V6 Manually
-- =====================================================
-- This script applies the missing migrations:
-- V5: Adds data quality tracking columns to ingestion_manifest
-- V6: Adds batch_id column to all staging tables

-- =====================================================
-- MIGRATION V5: Add Data Quality Tracking
-- =====================================================
-- Add data quality tracking columns to ingestion_manifest
ALTER TABLE title_d_app.ingestion_manifest 
ADD COLUMN IF NOT EXISTS corrected_records bigint DEFAULT 0,
ADD COLUMN IF NOT EXISTS warning_count integer DEFAULT 0,
ADD COLUMN IF NOT EXISTS error_count integer DEFAULT 0,
ADD COLUMN IF NOT EXISTS data_quality_status varchar(20) DEFAULT 'CLEAN';

-- Add comments for documentation
COMMENT ON COLUMN title_d_app.ingestion_manifest.corrected_records IS 'Number of records that were auto-corrected during validation (e.g., excess tabs converted to spaces)';
COMMENT ON COLUMN title_d_app.ingestion_manifest.warning_count IS 'Number of non-critical validation warnings found';
COMMENT ON COLUMN title_d_app.ingestion_manifest.error_count IS 'Number of validation errors found (non-blocking issues)';
COMMENT ON COLUMN title_d_app.ingestion_manifest.data_quality_status IS 'Overall data quality: CLEAN (no issues), CORRECTED (auto-fixed), WITH_WARNINGS (issues logged), WITH_ERRORS (errors logged), REJECTED (critical failures)';

-- Add index for reporting and monitoring queries
CREATE INDEX IF NOT EXISTS idx_manifest_data_quality 
ON title_d_app.ingestion_manifest(data_quality_status, created_at);

-- Add index for quality trend analysis
CREATE INDEX IF NOT EXISTS idx_manifest_quality_metrics 
ON title_d_app.ingestion_manifest(file_name, created_at, data_quality_status);

-- Update existing completed records to have CLEAN status (backwards compatibility)
UPDATE title_d_app.ingestion_manifest 
SET data_quality_status = 'CLEAN'
WHERE status = 'COMPLETED' AND data_quality_status IS NULL;

-- Record this migration in Flyway history (so Flyway knows it's been applied)
INSERT INTO flyway_schema_history (installed_rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success)
SELECT 
    COALESCE((SELECT MAX(installed_rank) FROM flyway_schema_history), 0) + 1,
    '5',
    'Add Data Quality Tracking',
    'SQL',
    'V5__Add_Data_Quality_Tracking.sql',
    -1234567890,  -- Placeholder checksum
    current_user,
    CURRENT_TIMESTAMP,
    100,
    true
WHERE NOT EXISTS (
    SELECT 1 FROM flyway_schema_history WHERE version = '5'
);

SELECT 'Migration V5 applied successfully!' AS status;

-- =====================================================
-- MIGRATION V6: Add Tracking Columns to Staging Tables
-- =====================================================
-- Add tracking columns (batch_id, row_number, loaded_at) to im1
ALTER TABLE title_d_app.im1 
ADD COLUMN IF NOT EXISTS batch_id UUID,
ADD COLUMN IF NOT EXISTS row_number BIGSERIAL,
ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add tracking columns to im2
ALTER TABLE title_d_app.im2 
ADD COLUMN IF NOT EXISTS batch_id UUID,
ADD COLUMN IF NOT EXISTS row_number BIGSERIAL,
ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add tracking columns to im3
ALTER TABLE title_d_app.im3 
ADD COLUMN IF NOT EXISTS batch_id UUID,
ADD COLUMN IF NOT EXISTS row_number BIGSERIAL,
ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add tracking columns to pm1
ALTER TABLE title_d_app.pm1 
ADD COLUMN IF NOT EXISTS batch_id UUID,
ADD COLUMN IF NOT EXISTS row_number BIGSERIAL,
ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add tracking columns to pm2
ALTER TABLE title_d_app.pm2 
ADD COLUMN IF NOT EXISTS batch_id UUID,
ADD COLUMN IF NOT EXISTS row_number BIGSERIAL,
ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add tracking columns to pm3
ALTER TABLE title_d_app.pm3 
ADD COLUMN IF NOT EXISTS batch_id UUID,
ADD COLUMN IF NOT EXISTS row_number BIGSERIAL,
ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add tracking columns to pm4
ALTER TABLE title_d_app.pm4 
ADD COLUMN IF NOT EXISTS batch_id UUID,
ADD COLUMN IF NOT EXISTS row_number BIGSERIAL,
ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add tracking columns to pm5
ALTER TABLE title_d_app.pm5 
ADD COLUMN IF NOT EXISTS batch_id UUID,
ADD COLUMN IF NOT EXISTS row_number BIGSERIAL,
ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add tracking columns to pm6
ALTER TABLE title_d_app.pm6 
ADD COLUMN IF NOT EXISTS batch_id UUID,
ADD COLUMN IF NOT EXISTS row_number BIGSERIAL,
ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add tracking columns to pm7
ALTER TABLE title_d_app.pm7 
ADD COLUMN IF NOT EXISTS batch_id UUID,
ADD COLUMN IF NOT EXISTS row_number BIGSERIAL,
ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Create indexes for efficient batch lookups
CREATE INDEX IF NOT EXISTS idx_im1_batch_id ON title_d_app.im1(batch_id);
CREATE INDEX IF NOT EXISTS idx_im2_batch_id ON title_d_app.im2(batch_id);
CREATE INDEX IF NOT EXISTS idx_im3_batch_id ON title_d_app.im3(batch_id);
CREATE INDEX IF NOT EXISTS idx_pm1_batch_id ON title_d_app.pm1(batch_id);
CREATE INDEX IF NOT EXISTS idx_pm2_batch_id ON title_d_app.pm2(batch_id);
CREATE INDEX IF NOT EXISTS idx_pm3_batch_id ON title_d_app.pm3(batch_id);
CREATE INDEX IF NOT EXISTS idx_pm4_batch_id ON title_d_app.pm4(batch_id);
CREATE INDEX IF NOT EXISTS idx_pm5_batch_id ON title_d_app.pm5(batch_id);
CREATE INDEX IF NOT EXISTS idx_pm6_batch_id ON title_d_app.pm6(batch_id);
CREATE INDEX IF NOT EXISTS idx_pm7_batch_id ON title_d_app.pm7(batch_id);

-- Add comments for documentation
COMMENT ON COLUMN title_d_app.im1.batch_id IS 'UUID linking to ingestion_manifest.batch_id for tracking data lineage';
COMMENT ON COLUMN title_d_app.im1.row_number IS 'Auto-incrementing row number for tracking row order';
COMMENT ON COLUMN title_d_app.im1.loaded_at IS 'Timestamp when the row was loaded into the table';

COMMENT ON COLUMN title_d_app.im2.batch_id IS 'UUID linking to ingestion_manifest.batch_id for tracking data lineage';
COMMENT ON COLUMN title_d_app.im2.row_number IS 'Auto-incrementing row number for tracking row order';
COMMENT ON COLUMN title_d_app.im2.loaded_at IS 'Timestamp when the row was loaded into the table';

COMMENT ON COLUMN title_d_app.im3.batch_id IS 'UUID linking to ingestion_manifest.batch_id for tracking data lineage';
COMMENT ON COLUMN title_d_app.im3.row_number IS 'Auto-incrementing row number for tracking row order';
COMMENT ON COLUMN title_d_app.im3.loaded_at IS 'Timestamp when the row was loaded into the table';

COMMENT ON COLUMN title_d_app.pm1.batch_id IS 'UUID linking to ingestion_manifest.batch_id for tracking data lineage';
COMMENT ON COLUMN title_d_app.pm1.row_number IS 'Auto-incrementing row number for tracking row order';
COMMENT ON COLUMN title_d_app.pm1.loaded_at IS 'Timestamp when the row was loaded into the table';

COMMENT ON COLUMN title_d_app.pm2.batch_id IS 'UUID linking to ingestion_manifest.batch_id for tracking data lineage';
COMMENT ON COLUMN title_d_app.pm2.row_number IS 'Auto-incrementing row number for tracking row order';
COMMENT ON COLUMN title_d_app.pm2.loaded_at IS 'Timestamp when the row was loaded into the table';

COMMENT ON COLUMN title_d_app.pm3.batch_id IS 'UUID linking to ingestion_manifest.batch_id for tracking data lineage';
COMMENT ON COLUMN title_d_app.pm3.row_number IS 'Auto-incrementing row number for tracking row order';
COMMENT ON COLUMN title_d_app.pm3.loaded_at IS 'Timestamp when the row was loaded into the table';

COMMENT ON COLUMN title_d_app.pm4.batch_id IS 'UUID linking to ingestion_manifest.batch_id for tracking data lineage';
COMMENT ON COLUMN title_d_app.pm4.row_number IS 'Auto-incrementing row number for tracking row order';
COMMENT ON COLUMN title_d_app.pm4.loaded_at IS 'Timestamp when the row was loaded into the table';

COMMENT ON COLUMN title_d_app.pm5.batch_id IS 'UUID linking to ingestion_manifest.batch_id for tracking data lineage';
COMMENT ON COLUMN title_d_app.pm5.row_number IS 'Auto-incrementing row number for tracking row order';
COMMENT ON COLUMN title_d_app.pm5.loaded_at IS 'Timestamp when the row was loaded into the table';

COMMENT ON COLUMN title_d_app.pm6.batch_id IS 'UUID linking to ingestion_manifest.batch_id for tracking data lineage';
COMMENT ON COLUMN title_d_app.pm6.row_number IS 'Auto-incrementing row number for tracking row order';
COMMENT ON COLUMN title_d_app.pm6.loaded_at IS 'Timestamp when the row was loaded into the table';

COMMENT ON COLUMN title_d_app.pm7.batch_id IS 'UUID linking to ingestion_manifest.batch_id for tracking data lineage';
COMMENT ON COLUMN title_d_app.pm7.row_number IS 'Auto-incrementing row number for tracking row order';
COMMENT ON COLUMN title_d_app.pm7.loaded_at IS 'Timestamp when the row was loaded into the table';

-- Record this migration in Flyway history (so Flyway knows it's been applied)
INSERT INTO flyway_schema_history (installed_rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success)
SELECT 
    COALESCE((SELECT MAX(installed_rank) FROM flyway_schema_history), 0) + 1,
    '6',
    'Add Tracking Columns To Staging Tables',
    'SQL',
    'V6__Add_Batch_Id_To_Staging_Tables.sql',
    -1234567891,  -- Placeholder checksum
    current_user,
    CURRENT_TIMESTAMP,
    100,
    true
WHERE NOT EXISTS (
    SELECT 1 FROM flyway_schema_history WHERE version = '6'
);

SELECT 'Migration V6 applied successfully!' AS status;

-- =====================================================
-- Final Verification
-- =====================================================
-- Verify ingestion_manifest columns
SELECT 'ingestion_manifest columns:' AS verification;
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'title_d_app'
AND table_name = 'ingestion_manifest'
AND column_name IN ('corrected_records', 'warning_count', 'error_count', 'data_quality_status')
ORDER BY column_name;

-- Verify staging tables have batch_id
SELECT 'Staging tables with batch_id:' AS verification;
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'title_d_app'
AND table_name IN ('im1', 'im2', 'im3', 'pm1', 'pm2', 'pm3', 'pm4', 'pm5', 'pm6', 'pm7')
AND column_name = 'batch_id'
ORDER BY table_name;

-- Success message
SELECT 'All migrations applied successfully!' AS status;
