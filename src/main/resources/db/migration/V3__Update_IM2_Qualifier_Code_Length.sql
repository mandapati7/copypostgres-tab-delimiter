-- Update staging_im2 qualifier_code column to accommodate date values (e.g., "0000/00/00")
ALTER TABLE title_d_app.staging_im2 
ALTER COLUMN qualifier_code TYPE VARCHAR(10);

-- Update title_d_app.im2 qualifier_code column for consistency
ALTER TABLE title_d_app.im2 
ALTER COLUMN qualifier_code TYPE VARCHAR(10);
