-- =====================================================
-- Add Tracking Columns to All Staging Tables
-- =====================================================
-- This migration adds tracking columns to all staging tables:
-- - batch_id: UUID linking to ingestion_manifest for data lineage
-- - row_number: Auto-incrementing row number for each row
-- - loaded_at: Timestamp when the row was loaded

-- Add tracking columns to im1
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
