-- Update staging_im2 qualifier_code column to accommodate date values (e.g., "0000/00/00")
ALTER TABLE title_d_app.staging_im2 
ALTER COLUMN qualifier_code TYPE VARCHAR(10);

-- Update title_d_app.im2 qualifier_code column for consistency
ALTER TABLE title_d_app.im2 
ALTER COLUMN qualifier_code TYPE VARCHAR(10);

-- Verify the changes
SELECT column_name, data_type, character_maximum_length 
FROM information_schema.columns 
WHERE table_schema = 'title_d_app' 
  AND table_name IN ('im2', 'staging_im2')
  AND column_name = 'qualifier_code';
