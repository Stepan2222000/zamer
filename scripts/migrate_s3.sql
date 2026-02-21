-- Migration: S3 Image Storage
-- Images stored in MinIO S3, only s3_keys kept in DB. BYTEA column removed.

ALTER TABLE catalog_listings
    ADD COLUMN IF NOT EXISTS s3_keys TEXT[];

ALTER TABLE catalog_listings
    DROP COLUMN IF EXISTS images_bytes;
