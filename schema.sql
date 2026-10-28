-- ==============================================================================
-- AXIE TREASURY DATABASE SCHEMA (PostgreSQL)
-- Sincronizado automaticamente
-- ==============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ------------------------------------------------------------------------------
-- Tabela: BAXSdaily
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "BAXSdaily" (
    "id" VARCHAR(64) PRIMARY KEY DEFAULT (gen_random_uuid())::text,
    "date" VARCHAR(32) NOT NULL,
    "Forge" DOUBLE PRECISION DEFAULT 0,
    "Evolve" DOUBLE PRECISION DEFAULT 0,
    "bAXS Fee" DOUBLE PRECISION DEFAULT 0,
    "Breeding" DOUBLE PRECISION DEFAULT 0,
    "Atia's Restore" DOUBLE PRECISION DEFAULT 0,
    "Runes & Charms" DOUBLE PRECISION DEFAULT 0,
    "Ascend" DOUBLE PRECISION DEFAULT 0
);

CREATE UNIQUE INDEX "BAXSdaily_date_key" ON public."BAXSdaily" USING btree (date);
CREATE INDEX "idx_BAXSdaily_date" ON public."BAXSdaily" USING btree (date);

-- ------------------------------------------------------------------------------
-- Tabela: BAXSsum
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "BAXSsum" (
    "id" VARCHAR(64) PRIMARY KEY,
    "Forge" DOUBLE PRECISION DEFAULT 0,
    "Evolve" DOUBLE PRECISION DEFAULT 0,
    "bAXS Fee" DOUBLE PRECISION DEFAULT 0,
    "Breeding" DOUBLE PRECISION DEFAULT 0,
    "Atia's Restore" DOUBLE PRECISION DEFAULT 0,
    "Runes & Charms" DOUBLE PRECISION DEFAULT 0,
    "Ascend" DOUBLE PRECISION DEFAULT 0,
    "absolute_total" DOUBLE PRECISION DEFAULT 0,
    "last_update" TIMESTAMPTZ
);

-- ------------------------------------------------------------------------------
-- Tabela: circulatingSupply
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "circulatingSupply" (
    "id" VARCHAR(64) PRIMARY KEY DEFAULT (gen_random_uuid())::text,
    "circulating_supply" DOUBLE PRECISION NOT NULL,
    "datetime" VARCHAR(32) NOT NULL,
    "timestamp" BIGINT NOT NULL
);

CREATE INDEX "idx_circulatingSupply_timestamp" ON public."circulatingSupply" USING btree ("timestamp");

-- ------------------------------------------------------------------------------
-- Tabela: dailyAXS
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "dailyAXS" (
    "id" VARCHAR(64) PRIMARY KEY DEFAULT (gen_random_uuid())::text,
    "date" VARCHAR(32) NOT NULL,
    "Forge" DOUBLE PRECISION DEFAULT 0,
    "Evolve" DOUBLE PRECISION DEFAULT 0,
    "Other" DOUBLE PRECISION DEFAULT 0,
    "bAXS Fee" DOUBLE PRECISION DEFAULT 0,
    "Breeding" DOUBLE PRECISION DEFAULT 0,
    "Atia's Restore" DOUBLE PRECISION DEFAULT 0,
    "Runes & Charms" DOUBLE PRECISION DEFAULT 0,
    "Ascend" DOUBLE PRECISION DEFAULT 0
);

CREATE UNIQUE INDEX "dailyAXS_date_key" ON public."dailyAXS" USING btree (date);
CREATE INDEX "idx_dailyAXS_date" ON public."dailyAXS" USING btree (date);

-- ------------------------------------------------------------------------------
-- Tabela: dailyData
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "dailyData" (
    "id" VARCHAR(64) PRIMARY KEY DEFAULT (gen_random_uuid())::text,
    "date" VARCHAR(32) NOT NULL,
    "entries" JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE UNIQUE INDEX "dailyData_date_key" ON public."dailyData" USING btree (date);
CREATE INDEX "idx_dailyData_date" ON public."dailyData" USING btree (date);

-- ------------------------------------------------------------------------------
-- Tabela: methods
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "methods" (
    "id" VARCHAR(64) PRIMARY KEY,
    "data" JSONB NOT NULL
);

-- ------------------------------------------------------------------------------
-- Tabela: overviewData
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "overviewData" (
    "id" VARCHAR(64) PRIMARY KEY DEFAULT (gen_random_uuid())::text,
    "date" VARCHAR(32) NOT NULL,
    "AXS" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "RON" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "USDC" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "WETH" DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX "overviewData_date_key" ON public."overviewData" USING btree (date);
CREATE INDEX "idx_overviewData_date" ON public."overviewData" USING btree (date);

-- ------------------------------------------------------------------------------
-- Tabela: stakingAXS
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "stakingAXS" (
    "id" VARCHAR(64) PRIMARY KEY DEFAULT (gen_random_uuid())::text,
    "date" VARCHAR(32) NOT NULL,
    "hour" VARCHAR(16) NOT NULL,
    "timestamp" BIGINT NOT NULL,
    "axs_total" DOUBLE PRECISION NOT NULL
);

CREATE INDEX "idx_stakingAXS_timestamp" ON public."stakingAXS" USING btree ("timestamp");
CREATE INDEX "idx_stakingAXS_date" ON public."stakingAXS" USING btree (date);

-- ------------------------------------------------------------------------------
-- Tabela: sumAXS
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "sumAXS" (
    "id" VARCHAR(64) PRIMARY KEY,
    "Forge" DOUBLE PRECISION DEFAULT 0,
    "Evolve" DOUBLE PRECISION DEFAULT 0,
    "Other" DOUBLE PRECISION DEFAULT 0,
    "bAXS Fee" DOUBLE PRECISION DEFAULT 0,
    "Breeding" DOUBLE PRECISION DEFAULT 0,
    "Atia's Restore" DOUBLE PRECISION DEFAULT 0,
    "Runes & Charms" DOUBLE PRECISION DEFAULT 0,
    "Ascend" DOUBLE PRECISION DEFAULT 0,
    "absolute_total" DOUBLE PRECISION DEFAULT 0,
    "last_update" TIMESTAMPTZ
);

-- ------------------------------------------------------------------------------
-- Tabela: tokens
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "tokens" (
    "id" VARCHAR(64) PRIMARY KEY,
    "data" JSONB NOT NULL
);

-- ------------------------------------------------------------------------------
-- Tabela: txns
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "txns" (
    "id" VARCHAR(100) PRIMARY KEY,
    "tx_hash" VARCHAR(66) NOT NULL,
    "block_number" BIGINT NOT NULL,
    "log_index" INT NOT NULL,
    "timestamp" BIGINT NOT NULL,
    "from_address" VARCHAR(42) NOT NULL,
    "to_address" VARCHAR(42) NOT NULL,
    "token_address" VARCHAR(42) NOT NULL,
    "amount_raw" NUMERIC(78) NOT NULL,
    "amount" NUMERIC(78) NOT NULL,
    "is_outgoing" BOOLEAN NOT NULL DEFAULT false,
    "method" VARCHAR(32) NOT NULL,
    "event_topic" VARCHAR(66)
);

CREATE INDEX idx_txns_block_number ON public.txns USING btree (block_number);
CREATE INDEX idx_txns_timestamp ON public.txns USING btree ("timestamp");
CREATE INDEX idx_txns_method ON public.txns USING btree (method);
CREATE INDEX idx_txns_tx_hash ON public.txns USING btree (tx_hash);
CREATE INDEX idx_txns_from_address ON public.txns USING btree (lower((from_address)::text));
CREATE INDEX idx_txns_to_address ON public.txns USING btree (lower((to_address)::text));
CREATE INDEX idx_txns_token_address ON public.txns USING btree (lower((token_address)::text));
CREATE INDEX idx_txns_baxs_query ON public.txns USING btree (lower((from_address)::text), lower((token_address)::text), method, "timestamp");

-- ------------------------------------------------------------------------------
-- Tabela: typeData
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "typeData" (
    "id" VARCHAR(64) PRIMARY KEY,
    "Breeding" JSONB,
    "Core" JSONB,
    "Marketplace" JSONB,
    "Origins" JSONB,
    "Other" JSONB,
    "Pack" JSONB,
    "bAXS" JSONB
);

-- ==============================================================================
-- VIEWS DE CONVENIÊNCIA
-- ==============================================================================
CREATE OR REPLACE VIEW baxs_daily AS SELECT id,
    date,
    "Forge",
    "Evolve",
    "bAXS Fee",
    "Breeding",
    "Atia's Restore",
    "Runes & Charms",
    "Ascend"
   FROM "BAXSdaily";
CREATE OR REPLACE VIEW baxs_sum AS SELECT id,
    "Forge",
    "Evolve",
    "bAXS Fee",
    "Breeding",
    "Atia's Restore",
    "Runes & Charms",
    "Ascend",
    absolute_total,
    last_update
   FROM "BAXSsum";
CREATE OR REPLACE VIEW circulating_supply AS SELECT id,
    circulating_supply,
    datetime,
    "timestamp"
   FROM "circulatingSupply";
CREATE OR REPLACE VIEW daily_axs AS SELECT id,
    date,
    "Forge",
    "Evolve",
    "Other",
    "bAXS Fee",
    "Breeding",
    "Atia's Restore",
    "Runes & Charms",
    "Ascend"
   FROM "dailyAXS";
CREATE OR REPLACE VIEW daily_data AS SELECT id,
    date,
    entries
   FROM "dailyData";
CREATE OR REPLACE VIEW overview_data AS SELECT id,
    date,
    "AXS",
    "RON",
    "USDC",
    "WETH"
   FROM "overviewData";
CREATE OR REPLACE VIEW staking_axs AS SELECT id,
    date,
    hour,
    "timestamp",
    axs_total
   FROM "stakingAXS";
CREATE OR REPLACE VIEW sum_axs AS SELECT id,
    "Forge",
    "Evolve",
    "Other",
    "bAXS Fee",
    "Breeding",
    "Atia's Restore",
    "Runes & Charms",
    "Ascend",
    absolute_total,
    last_update
   FROM "sumAXS";
CREATE OR REPLACE VIEW type_data AS SELECT id,
    "Breeding",
    "Core",
    "Marketplace",
    "Origins",
    "Other",
    "Pack",
    "bAXS"
   FROM "typeData";
