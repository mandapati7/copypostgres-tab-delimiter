-- Add data cleaning configuration columns to file_validation_rules table
-- These columns enable the PTDDataCleaner-style validations

ALTER TABLE title_d_app.file_validation_rules
ADD COLUMN IF NOT EXISTS replace_control_chars BOOLEAN NOT NULL DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS replace_non_latin_chars BOOLEAN NOT NULL DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS collapse_consecutive_replaced BOOLEAN NOT NULL DEFAULT FALSE;

-- Add comments to explain the new columns
COMMENT ON COLUMN title_d_app.file_validation_rules.replace_control_chars IS 'Replace control characters (non-letter, non-digit, non-whitespace in BASIC_LATIN) with asterisk (*)';
COMMENT ON COLUMN title_d_app.file_validation_rules.replace_non_latin_chars IS 'Replace characters outside BASIC_LATIN Unicode block with asterisk (*)';
COMMENT ON COLUMN title_d_app.file_validation_rules.collapse_consecutive_replaced IS 'Collapse consecutive asterisks to a single asterisk after replacement';

-- Update existing rules to enable data cleaning for specific file patterns
-- Enable all data cleaning features for pm3, pm5, pm6, im2, im3 patterns
UPDATE title_d_app.file_validation_rules
SET 
    replace_control_chars = TRUE,
    replace_non_latin_chars = TRUE,
    collapse_consecutive_replaced = TRUE
WHERE file_pattern IN ('pm3', 'pm5', 'pm6', 'im2', 'im3');
