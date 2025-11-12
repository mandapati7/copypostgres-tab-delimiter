-- =====================================================
-- File Validation Tables
-- =====================================================
-- These tables support configurable file validation rules
-- and tracking of validation issues for reporting

-- Table: file_validation_rules
-- Stores validation rules for different file types
CREATE TABLE IF NOT EXISTS title_d_app.file_validation_rules (
    id BIGSERIAL PRIMARY KEY,
    file_pattern VARCHAR(100) NOT NULL UNIQUE,
    table_name VARCHAR(100),
    expected_tab_count INTEGER NOT NULL,
    validation_enabled BOOLEAN NOT NULL DEFAULT true,
    auto_fix_enabled BOOLEAN NOT NULL DEFAULT false,
    reject_on_violation BOOLEAN NOT NULL DEFAULT false,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system'
);

-- Index for fast lookup by file pattern
CREATE INDEX idx_validation_rules_pattern ON title_d_app.file_validation_rules(file_pattern);

-- Table: file_validation_issues
-- Tracks all validation issues found during processing
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
    auto_fixed BOOLEAN NOT NULL DEFAULT false,
    fix_description TEXT,
    original_line TEXT,
    corrected_line TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for efficient querying
CREATE INDEX idx_validation_batch ON title_d_app.file_validation_issues(batch_id);
CREATE INDEX idx_validation_severity ON title_d_app.file_validation_issues(severity);
CREATE INDEX idx_validation_created ON title_d_app.file_validation_issues(created_at);

-- Insert default validation rules based on your existing logic
INSERT INTO title_d_app.file_validation_rules (file_pattern, table_name, expected_tab_count, validation_enabled, auto_fix_enabled, reject_on_violation, description) VALUES
('PM3', 'pm3', 6, true, true, false, 'PM3 files should have 6 tabs per row (7 columns)'),
('PM5', 'pm5', 2, true, true, false, 'PM5 files should have 2 tabs per row (3 columns)'),
('PM6', 'pm6', 2, true, true, false, 'PM6 files should have 2 tabs per row (3 columns)'),
('IM2', 'im2', 8, true, true, false, 'IM2 files should have 8 tabs per row (9 columns)'),
('IM3', 'im3', 2, true, true, false, 'IM3 files should have 2 tabs per row (3 columns)'),
('PM1', 'pm1', 16, true, true, false, 'PM1 files should have 16 tabs per row (17 columns)'),
('PM2', 'pm2', 4, true, true, false, 'PM2 files should have 4 tabs per row (5 columns)'),
('PM4', 'pm4', 3, true, true, false, 'PM4 files should have 3 tabs per row (4 columns)'),
('PM7', 'pm7', 9, true, true, false, 'PM7 files should have 9 tabs per row (10 columns)'),
('IM1', 'im1', 12, true, true, false, 'IM1 files should have 12 tabs per row (13 columns)')
ON CONFLICT (file_pattern) DO NOTHING;

-- Comments for documentation
COMMENT ON TABLE title_d_app.file_validation_rules IS 'Configuration table for file validation rules';
COMMENT ON COLUMN title_d_app.file_validation_rules.file_pattern IS 'File pattern to match (e.g., PM3, IM2)';
COMMENT ON COLUMN title_d_app.file_validation_rules.expected_tab_count IS 'Expected number of tabs per row';
COMMENT ON COLUMN title_d_app.file_validation_rules.auto_fix_enabled IS 'Whether to automatically fix excess tabs';
COMMENT ON COLUMN title_d_app.file_validation_rules.reject_on_violation IS 'Whether to reject entire file on validation failure';
COMMENT ON TABLE title_d_app.file_validation_issues IS 'Tracks all validation issues found during file processing';
COMMENT ON COLUMN title_d_app.file_validation_issues.batch_id IS 'Links to ingestion_manifest.batch_id';
COMMENT ON COLUMN title_d_app.file_validation_issues.auto_fixed IS 'Whether the issue was automatically corrected';
COMMENT ON COLUMN title_d_app.file_validation_issues.fix_description IS 'Description of how the issue was fixed';