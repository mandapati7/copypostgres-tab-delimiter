-- =====================================================
-- Add Data Quality Tracking to Ingestion Manifest
-- =====================================================
-- This migration adds columns to track data quality metrics
-- for files processed through validation

-- Add data quality tracking columns
ALTER TABLE ingestion_manifest 
ADD COLUMN IF NOT EXISTS corrected_records bigint DEFAULT 0,
ADD COLUMN IF NOT EXISTS warning_count integer DEFAULT 0,
ADD COLUMN IF NOT EXISTS error_count integer DEFAULT 0,
ADD COLUMN IF NOT EXISTS data_quality_status varchar(20) DEFAULT 'CLEAN';

-- Add comments for documentation
COMMENT ON COLUMN ingestion_manifest.corrected_records IS 'Number of records that were auto-corrected during validation (e.g., excess tabs converted to spaces)';
COMMENT ON COLUMN ingestion_manifest.warning_count IS 'Number of non-critical validation warnings found';
COMMENT ON COLUMN ingestion_manifest.error_count IS 'Number of validation errors found (non-blocking issues)';
COMMENT ON COLUMN ingestion_manifest.data_quality_status IS 'Overall data quality: CLEAN (no issues), CORRECTED (auto-fixed), WITH_WARNINGS (issues logged), WITH_ERRORS (errors logged), REJECTED (critical failures)';

-- Add index for reporting and monitoring queries
CREATE INDEX IF NOT EXISTS idx_manifest_data_quality 
ON ingestion_manifest(data_quality_status, created_at);

-- Add index for quality trend analysis
CREATE INDEX IF NOT EXISTS idx_manifest_quality_metrics 
ON ingestion_manifest(file_name, created_at, data_quality_status);

-- Update existing completed records to have CLEAN status (backwards compatibility)
UPDATE ingestion_manifest 
SET data_quality_status = 'CLEAN'
WHERE status = 'COMPLETED' 
  AND data_quality_status IS NULL;

-- Update existing failed records (optional - for consistency)
UPDATE ingestion_manifest 
SET data_quality_status = 'REJECTED'
WHERE status = 'FAILED' 
  AND data_quality_status IS NULL;
