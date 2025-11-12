-- Script to reorder IM2 table columns to match source file structure
-- 
-- PROBLEM: 
-- The IM2 table columns are in alphabetical order, but the source file 
-- (from SQL*Loader) has columns in a different order, specifically:
-- - party_b_day is at position 4 in source file
-- - party_b_day is at position 9 in current database table
--
-- SOLUTION:
-- Recreate the table with columns in the correct order matching the source file
--
-- SOURCE FILE ORDER (from IM2_CONTROL_DAILY.CTL):
-- 1. lro_num
-- 2. instrument_num
-- 3. to_from_ind
-- 4. party_b_day          <- DATE FIELD at position 4
-- 5. qualifier_code
-- 6. qualifier_name
-- 7. capacity_code
-- 8. im_share
-- 9. name

-- BACKUP: First, let's back up any existing data
BEGIN;

-- Step 1: Rename the old table
ALTER TABLE title_d_app.im2 RENAME TO im2_old_backup;

-- Step 2: Create new table with correct column order
CREATE TABLE title_d_app.im2 (
    lro_num VARCHAR(2) NOT NULL,
    instrument_num VARCHAR(10) NOT NULL,
    to_from_ind VARCHAR(1) NULL,
    party_b_day TIMESTAMP(0) NULL,           -- Position 4 (matches source file)
    qualifier_code VARCHAR(6) NULL,
    qualifier_name VARCHAR(128) NULL,
    capacity_code VARCHAR(6) NULL,
    im_share VARCHAR(12) NULL,
    name VARCHAR(128) NULL,                  -- Position 9
    
    -- Metadata columns (auto-generated, not in source file)
    batch_id UUID NULL,
    row_number BIGINT NULL,
    loaded_at TIMESTAMP(0) NULL
);

-- Step 3: Recreate the index
CREATE INDEX im2_lro_instr ON title_d_app.im2 USING btree (instrument_num, lro_num);

-- Step 4: Create index on batch_id for performance
CREATE INDEX im2_batch_id_idx ON title_d_app.im2 USING btree (batch_id);

-- Step 5: Copy data from old table to new table (if any exists)
INSERT INTO title_d_app.im2 (
    lro_num, 
    instrument_num, 
    to_from_ind, 
    party_b_day,
    qualifier_code, 
    qualifier_name, 
    capacity_code, 
    im_share, 
    name,
    batch_id,
    row_number,
    loaded_at
)
SELECT 
    lro_num, 
    instrument_num, 
    to_from_ind, 
    party_b_day,
    qualifier_code, 
    qualifier_name, 
    capacity_code, 
    im_share, 
    name,
    batch_id,
    row_number,
    loaded_at
FROM title_d_app.im2_old_backup;

-- Step 6: Verify the data was copied correctly
DO $$
DECLARE
    old_count INTEGER;
    new_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO old_count FROM title_d_app.im2_old_backup;
    SELECT COUNT(*) INTO new_count FROM title_d_app.im2;
    
    IF old_count != new_count THEN
        RAISE EXCEPTION 'Row count mismatch! Old table: %, New table: %', old_count, new_count;
    END IF;
    
    RAISE NOTICE 'Data migration successful: % rows copied', new_count;
END $$;

-- Step 7: Verify column order is correct
SELECT 
    ordinal_position,
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'title_d_app'
  AND table_name = 'im2'
ORDER BY ordinal_position;

COMMIT;

-- After successful migration, you can drop the backup table:
-- DROP TABLE title_d_app.im2_old_backup;

-- IMPORTANT: 
-- Keep the backup table until you've verified everything works correctly!
-- Only drop it after successful testing with IM2 files.
