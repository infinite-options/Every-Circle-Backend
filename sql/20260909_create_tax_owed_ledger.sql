-- Tax owed liability ledger (additive only).
-- Mirrors wallet / wallet_transactions for seller sales-tax remittance tracking.
-- Personal vs business stay separate via tax_owed_profile_id / tt_profile_id
-- (same id rules as wallet_profile_id: profile_personal_uid or business_uid).

CREATE TABLE IF NOT EXISTS every_circle.tax_owed (
    tax_owed_profile_id VARCHAR(64) NOT NULL,
    tax_owed_balance DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
    tax_owed_lifetime_collected DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
    tax_owed_lifetime_reversed DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
    tax_owed_lifetime_remitted DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
    tax_owed_updated_at DATETIME NOT NULL,
    PRIMARY KEY (tax_owed_profile_id)
);

CREATE TABLE IF NOT EXISTS every_circle.tax_transactions (
    tt_uid VARCHAR(64) NOT NULL,
    tt_profile_id VARCHAR(64) NOT NULL,
    tt_buyer_id VARCHAR(64) NOT NULL,
    tt_seller_id VARCHAR(64) NOT NULL,
    tt_transaction_id VARCHAR(64) NOT NULL,
    tt_ti_id VARCHAR(64) NOT NULL,
    tt_type VARCHAR(32) NOT NULL,
    tt_status VARCHAR(32) NOT NULL DEFAULT 'posted',
    tt_qty INT NOT NULL,
    tt_amount DECIMAL(18,4) NOT NULL,
    tt_currency VARCHAR(8) NOT NULL,
    tt_tax_rate DECIMAL(18,6) NULL,
    tt_idempotency_key VARCHAR(128) NOT NULL,
    tt_note VARCHAR(512) NULL,
    tt_created_at DATETIME NOT NULL,
    tt_updated_at DATETIME NOT NULL,
    PRIMARY KEY (tt_uid),
    UNIQUE KEY uq_tt_idempotency (tt_idempotency_key),
    KEY idx_tt_profile_id (tt_profile_id),
    KEY idx_tt_transaction_id (tt_transaction_id),
    KEY idx_tt_ti_id (tt_ti_id)
);

-- UID allocator (prefix 570-*), same style as new_wallet_transaction_uid (560-*).
DROP PROCEDURE IF EXISTS every_circle.new_tax_transaction_uid;
DELIMITER //
CREATE PROCEDURE every_circle.new_tax_transaction_uid()
BEGIN
    DECLARE last_id varchar(50);
    DECLARE new_id varchar(50);
    DECLARE prefix varchar(50);
    DECLARE suffix varchar(50);

    SET last_id = (SELECT tt_uid FROM every_circle.tax_transactions ORDER BY tt_uid DESC LIMIT 1);
    SET prefix = '570';
    IF last_id IS NULL THEN
        SET suffix = '0';
    ELSE
        SET suffix = CONVERT(SUBSTRING_INDEX(last_id,'-',-1),char(6));
    END IF;

    SET suffix = suffix+1;
    SET suffix = LPAD(suffix,6,0);

    SET new_id = CONCAT(prefix, '-', suffix);
    SELECT new_id;

END //
DELIMITER ;
