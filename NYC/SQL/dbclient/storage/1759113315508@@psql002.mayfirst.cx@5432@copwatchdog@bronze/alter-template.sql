-- change column type
ALTER TABLE doberman_fetch 
    ALTER COLUMN "date" TYPE text;
-- change column name
ALTER TABLE doberman_fetch 
    RENAME COLUMN "date" TO "date";
-- Change column comment
COMMENT ON COLUMN doberman_fetch."date" IS 'comment';