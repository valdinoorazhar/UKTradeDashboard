-- Drop column if it exists for idempotency
ALTER TABLE TRADE_STAGING.trade DROP COLUMN IF EXISTS {column_name};

-- Add new column
ALTER TABLE TRADE_STAGING.trade ADD COLUMN {} Decimal (18, 4) NULL;