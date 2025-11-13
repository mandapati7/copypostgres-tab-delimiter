-- =====================================================
-- COMPLETE DATABASE SCHEMA SETUP
-- =====================================================
-- This script consolidates all migrations into a single file
-- for easier management and deployment
--
-- Features included:
-- 1. Title D App schema creation
-- 2. Title D App tables (PM1-PM7, IM1-IM3)
-- 3. File validation rules and tracking
-- 4. Data quality tracking
-- 5. Data transformation configuration
-- 6. Batch tracking columns
-- 7. Data cleaning rules
--
-- Usage: Run this entire script on a fresh database OR
--        comment out sections already applied to your database

-- =====================================================
-- SECTION 1: CREATE SCHEMA
-- =====================================================

CREATE SCHEMA IF NOT EXISTS title_d_app;

-- =====================================================
-- SECTION 2: INGESTION MANIFEST TABLE
-- =====================================================

CREATE TABLE IF NOT EXISTS title_d_app.ingestion_manifest (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    file_name VARCHAR(255) NOT NULL,
    file_path VARCHAR(500),
    file_size_bytes BIGINT NOT NULL,
    file_checksum VARCHAR(64) NOT NULL,
    content_type VARCHAR(100),
    table_name VARCHAR(100),
    
    -- Processing metadata
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    total_records BIGINT DEFAULT 0,
    processed_records BIGINT DEFAULT 0,
    failed_records BIGINT DEFAULT 0,
    
    -- Data quality tracking
    corrected_records BIGINT DEFAULT 0,
    warning_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    data_quality_status VARCHAR(20) DEFAULT 'CLEAN',
    
    -- Timing information
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    processing_duration_ms BIGINT,
    
    -- Error handling
    error_message TEXT,
    error_details TEXT,
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system',
    
    -- Constraints
    CONSTRAINT chk_file_size CHECK (file_size_bytes >= 0),
    CONSTRAINT chk_records_non_negative CHECK (
        total_records >= 0 AND 
        processed_records >= 0 AND 
        failed_records >= 0 AND
        processed_records + failed_records <= total_records
    )
);

-- Add data quality columns to existing table
ALTER TABLE title_d_app.ingestion_manifest 
ADD COLUMN IF NOT EXISTS corrected_records BIGINT DEFAULT 0,
ADD COLUMN IF NOT EXISTS warning_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS error_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS data_quality_status VARCHAR(20) DEFAULT 'CLEAN',
ADD COLUMN IF NOT EXISTS table_name VARCHAR(100);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_manifest_batch_id ON title_d_app.ingestion_manifest(batch_id);
CREATE INDEX IF NOT EXISTS idx_manifest_status ON title_d_app.ingestion_manifest(status);
CREATE INDEX IF NOT EXISTS idx_manifest_checksum ON title_d_app.ingestion_manifest(file_checksum);
CREATE INDEX IF NOT EXISTS idx_manifest_data_quality ON title_d_app.ingestion_manifest(data_quality_status, created_at);
CREATE INDEX IF NOT EXISTS idx_manifest_quality_metrics ON title_d_app.ingestion_manifest(file_name, created_at, data_quality_status);

-- =====================================================
-- SECTION 4: TITLE D APP TABLES (PM1-PM7, IM1-IM3)
-- =====================================================

-- PM1: Property Master Table
DROP TABLE IF EXISTS title_d_app.pm1 CASCADE;
CREATE TABLE title_d_app.pm1 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    registration_system_code VARCHAR(1) NULL,
    property_type_code VARCHAR(1) NULL,
    last_update_date TIMESTAMP(0) NULL,
    interest_code VARCHAR(6) NULL,
    qualifier_code VARCHAR(6) NULL,
    origin_code VARCHAR(1) NULL,
    status_code VARCHAR(1) NULL,
    parent_block VARCHAR(5) NULL,
    parent_property_id VARCHAR(4) NULL,
    parent_count NUMERIC NULL,
    child_from_block VARCHAR(5) NULL,
    child_from_property VARCHAR(4) NULL,
    child_to_block VARCHAR(5) NULL,
    child_to_property VARCHAR(4) NULL,
    polaris_restriction_ind VARCHAR(1) NULL,
    
    -- Tracking columns
    batch_id UUID NULL,
    row_number BIGSERIAL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_pm1_blk_pid_batch ON title_d_app.pm1 USING btree (batch_id, block_num, property_id_num);
CREATE INDEX IF NOT EXISTS idx_pm1_batch_id ON title_d_app.pm1(batch_id);

-- PM2: Property-Instrument Relationship
DROP TABLE IF EXISTS title_d_app.pm2 CASCADE;
CREATE TABLE title_d_app.pm2 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    lro_num VARCHAR(2) NOT NULL,
    instrument_num VARCHAR(10) NOT NULL,
    rule_out_ind VARCHAR(1) NULL,
    seq_id NUMERIC(38) NULL,
    
    -- Tracking columns
    batch_id UUID NULL,
    row_number BIGSERIAL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_pm2_seq_id_batch ON title_d_app.pm2 USING btree (batch_id, seq_id);
CREATE INDEX IF NOT EXISTS idx_pm2_batch_id ON title_d_app.pm2(batch_id);

-- PM3: Alternative Party Information
DROP TABLE IF EXISTS title_d_app.pm3 CASCADE;
CREATE TABLE title_d_app.pm3 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    alt_party_id_code VARCHAR(6) NULL,
    alt_party_name VARCHAR(128) NULL,
    owner_capacity_code VARCHAR(6) NULL,
    ownership_share_desc VARCHAR(12) NULL,
    owner_name VARCHAR(128) NULL,
    alt_party_b_day TIMESTAMP(0) NULL,
    
    -- Tracking columns
    batch_id UUID NULL,
    row_number BIGSERIAL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_pm3_blk_pid_batch ON title_d_app.pm3 USING btree (batch_id, block_num, property_id_num);
CREATE INDEX IF NOT EXISTS idx_pm3_batch_id ON title_d_app.pm3(batch_id);

-- PM4: Parent Property Relationships
DROP TABLE IF EXISTS title_d_app.pm4 CASCADE;
CREATE TABLE title_d_app.pm4 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    parent_block_num VARCHAR(5) NOT NULL,
    parent_property_id_num VARCHAR(4) NOT NULL,
    
    -- Tracking columns
    batch_id UUID NULL,
    row_number BIGSERIAL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_pm4_blk_pid_batch ON title_d_app.pm4 USING btree (batch_id, block_num, property_id_num);
CREATE INDEX IF NOT EXISTS idx_pm4_batch_id ON title_d_app.pm4(batch_id);

-- PM5: Property Thumbnail/Description
DROP TABLE IF EXISTS title_d_app.pm5 CASCADE;
CREATE TABLE title_d_app.pm5 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    thumbnail_desc TEXT NULL,
    
    -- Tracking columns
    batch_id UUID NULL,
    row_number BIGSERIAL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_pm5_blk_pid_batch ON title_d_app.pm5 USING btree (batch_id, block_num, property_id_num);
CREATE INDEX IF NOT EXISTS idx_pm5_batch_id ON title_d_app.pm5(batch_id);

-- PM6: Property Comments
DROP TABLE IF EXISTS title_d_app.pm6 CASCADE;
CREATE TABLE title_d_app.pm6 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    comment_desc VARCHAR(255) NULL,
    
    -- Tracking columns
    batch_id UUID NULL,
    row_number BIGSERIAL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_pm6_blk_pid_batch ON title_d_app.pm6 USING btree (batch_id, block_num, property_id_num);
CREATE INDEX IF NOT EXISTS idx_pm6_batch_id ON title_d_app.pm6(batch_id);

-- PM7: Property Address Information
DROP TABLE IF EXISTS title_d_app.pm7 CASCADE;
CREATE TABLE title_d_app.pm7 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    assessment_num VARCHAR(15) NULL,
    municipality_name VARCHAR(24) NULL,
    postal_region_name VARCHAR(26) NULL,
    street_num NUMERIC NULL,
    street_suffix VARCHAR(6) NULL,
    street_name VARCHAR(34) NULL,
    unit_num VARCHAR(6) NULL,
    unit_type_code VARCHAR(6) NULL,
    
    -- Tracking columns
    batch_id UUID NULL,
    row_number BIGSERIAL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_pm7_blk_pid_batch ON title_d_app.pm7 USING btree (batch_id, block_num, property_id_num);
CREATE INDEX IF NOT EXISTS idx_pm7_batch_id ON title_d_app.pm7(batch_id);

-- IM1: Instrument Master Table
DROP TABLE IF EXISTS title_d_app.im1 CASCADE;
CREATE TABLE title_d_app.im1 (
    lro_num VARCHAR(2) NOT NULL,
    instrument_num VARCHAR(10) NOT NULL,
    registration_date TIMESTAMP(0) NULL,
    interest_code VARCHAR(6) NULL,
    qualifier_code VARCHAR(6) NULL,
    ref_charge_instr_num VARCHAR(10) NULL,
    ref_assignment_instr_num VARCHAR(10) NULL,
    consideration_amt NUMERIC NULL,
    last_update_date TIMESTAMP(0) NULL,
    instrument_selection_code VARCHAR(1) NULL,
    instrument_sub_type_code VARCHAR(8) NULL,
    origin_ind_code VARCHAR(1) NULL,
    instrument_registration_stage VARCHAR(1) NULL,
    
    -- Tracking columns
    batch_id UUID NULL,
    row_number BIGSERIAL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_im1_lro_instr_batch ON title_d_app.im1 USING btree (batch_id, instrument_num, lro_num);
CREATE INDEX IF NOT EXISTS idx_im1_batch_id ON title_d_app.im1(batch_id);

-- IM2: Instrument Party Information
-- NOTE: This table structure matches the SOURCE FILE order (not alphabetical)
-- party_b_day is at position 9 (matches actual database schema)
DROP TABLE IF EXISTS title_d_app.im2 CASCADE;
CREATE TABLE title_d_app.im2 (
    lro_num VARCHAR(2) NOT NULL,
    instrument_num VARCHAR(10) NOT NULL,
    to_from_ind VARCHAR(1) NULL,
    party_b_day TIMESTAMP(0) NULL,           -- Position 9 (matches actual database)
    qualifier_code VARCHAR(6) NULL,
    qualifier_name VARCHAR(128) NULL,
    capacity_code VARCHAR(6) NULL,
    im_share VARCHAR(12) NULL,
    name VARCHAR(128) NULL,
    
    -- Tracking columns
    batch_id UUID NULL,
    row_number BIGSERIAL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_im2_lro_instr_batch ON title_d_app.im2 USING btree (batch_id, instrument_num, lro_num);
CREATE INDEX IF NOT EXISTS idx_im2_batch_id ON title_d_app.im2(batch_id);

-- IM3: Instrument Comments
DROP TABLE IF EXISTS title_d_app.im3 CASCADE;
CREATE TABLE title_d_app.im3 (
    lro_num VARCHAR(2) NOT NULL,
    instrument_num VARCHAR(10) NOT NULL,
    comment_desc VARCHAR(255) NULL,
    
    -- Tracking columns
    batch_id UUID NULL,
    row_number BIGSERIAL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_im3_lro_instr_batch ON title_d_app.im3 USING btree (batch_id, instrument_num, lro_num);
CREATE INDEX IF NOT EXISTS idx_im3_batch_id ON title_d_app.im3(batch_id);

-- =====================================================
-- SECTION 5: FILE VALIDATION TABLES
-- =====================================================

-- File Validation Rules
CREATE TABLE IF NOT EXISTS title_d_app.file_validation_rules (
    id BIGSERIAL PRIMARY KEY,
    file_pattern VARCHAR(100) NOT NULL UNIQUE,
    table_name VARCHAR(100),
    expected_tab_count INTEGER NOT NULL,
    validation_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    auto_fix_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    reject_on_violation BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Data cleaning configuration
    replace_control_chars BOOLEAN NOT NULL DEFAULT FALSE,
    replace_non_latin_chars BOOLEAN NOT NULL DEFAULT FALSE,
    collapse_consecutive_replaced BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Data transformation configuration
    enable_data_transformation BOOLEAN NOT NULL DEFAULT FALSE,
    transformer_class_name VARCHAR(255),
    
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system'
);

CREATE INDEX IF NOT EXISTS idx_validation_rules_pattern ON title_d_app.file_validation_rules(file_pattern);

-- File Validation Issues
CREATE TABLE IF NOT EXISTS title_d_app.file_validation_issues (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    line_number BIGINT NOT NULL,
    issue_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    expected_value VARCHAR(500),
    actual_value VARCHAR(500),
    description TEXT,
    auto_fixed BOOLEAN NOT NULL DEFAULT FALSE,
    fix_description TEXT,
    original_line TEXT,
    corrected_line TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_validation_batch ON title_d_app.file_validation_issues(batch_id);
CREATE INDEX IF NOT EXISTS idx_validation_severity ON title_d_app.file_validation_issues(severity);
CREATE INDEX IF NOT EXISTS idx_validation_created ON title_d_app.file_validation_issues(created_at);

-- =====================================================
-- SECTION 6: INSERT DEFAULT VALIDATION RULES
-- =====================================================

-- Insert or update validation rules
INSERT INTO title_d_app.file_validation_rules (
    file_pattern, table_name, expected_tab_count, 
    validation_enabled, auto_fix_enabled, reject_on_violation,
    replace_control_chars, replace_non_latin_chars, collapse_consecutive_replaced,
    enable_data_transformation, transformer_class_name,
    description
) VALUES
    ('PM1', 'pm1', 16, TRUE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, NULL, 'PM1 files should have 16 tabs per row (17 columns)'),
    ('PM2', 'pm2', 4, TRUE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, NULL, 'PM2 files should have 4 tabs per row (5 columns)'),
    ('PM3', 'pm3', 6, TRUE, TRUE, FALSE, TRUE, TRUE, TRUE, FALSE, NULL, 'PM3 files should have 6 tabs per row (7 columns)'),
    ('PM4', 'pm4', 3, TRUE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, NULL, 'PM4 files should have 3 tabs per row (4 columns)'),
    ('PM5', 'pm5', 2, TRUE, TRUE, FALSE, TRUE, TRUE, TRUE, FALSE, NULL, 'PM5 files should have 2 tabs per row (3 columns)'),
    ('PM6', 'pm6', 2, TRUE, TRUE, FALSE, TRUE, TRUE, TRUE, FALSE, NULL, 'PM6 files should have 2 tabs per row (3 columns)'),
    ('PM7', 'pm7', 9, TRUE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, NULL, 'PM7 files should have 9 tabs per row (10 columns)'),
    ('IM1', 'im1', 12, TRUE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, NULL, 'IM1 files should have 12 tabs per row (13 columns)'),
    ('IM2', 'im2', 8, TRUE, TRUE, FALSE, TRUE, TRUE, TRUE, TRUE, 'teranet.mapdev.ingest.transformer.IM2Transformer', 'IM2 files should have 8 tabs per row (9 columns) - includes date transformation'),
    ('IM3', 'im3', 2, TRUE, TRUE, FALSE, TRUE, TRUE, TRUE, FALSE, NULL, 'IM3 files should have 2 tabs per row (3 columns)')
ON CONFLICT (file_pattern) DO UPDATE SET
    table_name = EXCLUDED.table_name,
    expected_tab_count = EXCLUDED.expected_tab_count,
    validation_enabled = EXCLUDED.validation_enabled,
    auto_fix_enabled = EXCLUDED.auto_fix_enabled,
    reject_on_violation = EXCLUDED.reject_on_violation,
    replace_control_chars = EXCLUDED.replace_control_chars,
    replace_non_latin_chars = EXCLUDED.replace_non_latin_chars,
    collapse_consecutive_replaced = EXCLUDED.collapse_consecutive_replaced,
    enable_data_transformation = EXCLUDED.enable_data_transformation,
    transformer_class_name = EXCLUDED.transformer_class_name,
    description = EXCLUDED.description,
    updated_at = CURRENT_TIMESTAMP;

-- =====================================================
-- SECTION 7: FUNCTIONS AND TRIGGERS
-- =====================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for ingestion_manifest
DROP TRIGGER IF EXISTS trigger_update_ingestion_manifest_updated_at ON title_d_app.ingestion_manifest;
CREATE TRIGGER trigger_update_ingestion_manifest_updated_at
    BEFORE UPDATE ON title_d_app.ingestion_manifest
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger for file_validation_rules
DROP TRIGGER IF EXISTS trigger_update_validation_rules_updated_at ON title_d_app.file_validation_rules;
CREATE TRIGGER trigger_update_validation_rules_updated_at
    BEFORE UPDATE ON title_d_app.file_validation_rules
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- SECTION 8: COMMENTS FOR DOCUMENTATION
-- =====================================================

COMMENT ON TABLE title_d_app.ingestion_manifest IS 'Tracks all file ingestion attempts with processing status and metrics';
COMMENT ON TABLE title_d_app.file_validation_rules IS 'Configuration for file validation rules per file pattern';
COMMENT ON TABLE title_d_app.file_validation_issues IS 'Tracks all validation issues found during file processing';

COMMENT ON TABLE title_d_app.pm1 IS 'Property Master Table - Core property information';
COMMENT ON TABLE title_d_app.pm2 IS 'Property-Instrument Relationship - Links properties to instruments';
COMMENT ON TABLE title_d_app.pm3 IS 'Alternative Party Information - Alternate party details and ownership';
COMMENT ON TABLE title_d_app.pm4 IS 'Parent Property Relationships - Hierarchical property structure';
COMMENT ON TABLE title_d_app.pm5 IS 'Property Thumbnails - Visual or text descriptions';
COMMENT ON TABLE title_d_app.pm6 IS 'Property Comments - Additional property notes';
COMMENT ON TABLE title_d_app.pm7 IS 'Property Address Information - Detailed location data';

COMMENT ON TABLE title_d_app.im1 IS 'Instrument Master Table - Core instrument registration data';
COMMENT ON TABLE title_d_app.im2 IS 'Instrument Party Information - Party relationships with SOURCE FILE column order (party_b_day at position 4)';
COMMENT ON TABLE title_d_app.im3 IS 'Instrument Comments - Additional instrument notes';

COMMENT ON COLUMN title_d_app.ingestion_manifest.corrected_records IS 'Number of records auto-corrected during validation';
COMMENT ON COLUMN title_d_app.ingestion_manifest.data_quality_status IS 'Overall data quality: CLEAN, CORRECTED, WITH_WARNINGS, WITH_ERRORS, REJECTED';

COMMENT ON COLUMN title_d_app.file_validation_rules.replace_control_chars IS 'Replace control characters with asterisk (*)';
COMMENT ON COLUMN title_d_app.file_validation_rules.replace_non_latin_chars IS 'Replace non-BASIC_LATIN characters with asterisk (*)';
COMMENT ON COLUMN title_d_app.file_validation_rules.collapse_consecutive_replaced IS 'Collapse consecutive asterisks to single asterisk';
COMMENT ON COLUMN title_d_app.file_validation_rules.enable_data_transformation IS 'Enable custom data transformations for this file pattern';
COMMENT ON COLUMN title_d_app.file_validation_rules.transformer_class_name IS 'Fully qualified Java class name implementing DataTransformer interface';

-- =====================================================
-- SECTION 9: VERIFY SETUP
-- =====================================================

-- Display summary of created objects
DO $$
DECLARE
    table_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO table_count 
    FROM information_schema.tables 
    WHERE table_schema = 'title_d_app';
    
    RAISE NOTICE '=====================================================';
    RAISE NOTICE 'SCHEMA SETUP COMPLETE';
    RAISE NOTICE '=====================================================';
    RAISE NOTICE 'Total tables in title_d_app schema: %', table_count;
    RAISE NOTICE '=====================================================';
    
    -- List all tables
    RAISE NOTICE 'Tables created:';
    FOR table_name IN 
        SELECT t.table_name 
        FROM information_schema.tables t
        WHERE t.table_schema = 'title_d_app'
        ORDER BY t.table_name
    LOOP
        RAISE NOTICE '  - %', table_name;
    END LOOP;
END $$;

-- Verify column order for IM2 (should show party_b_day at position 4)
SELECT 
    ordinal_position,
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'title_d_app'
  AND table_name = 'im2'
  AND ordinal_position <= 9
ORDER BY ordinal_position;
