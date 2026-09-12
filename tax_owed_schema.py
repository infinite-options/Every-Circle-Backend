"""
Schema ensure for tax owed liability ledger.

Additive only: creates tax_owed + tax_transactions if missing, and quietly
adds any missing columns/indexes on older installs. Does not alter or drop
existing wallet / transaction / listing columns.
"""

_TAX_OWED_TABLES_READY = False

_NEW_TAX_TRANSACTION_UID_PROCEDURE = """
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

END
"""


def ensure_tax_owed_tables(db):
    """Create tax_owed / tax_transactions once per process if missing."""
    global _TAX_OWED_TABLES_READY
    if _TAX_OWED_TABLES_READY:
        return

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS every_circle.tax_owed (
            tax_owed_profile_id VARCHAR(64) NOT NULL,
            tax_owed_balance DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
            tax_owed_lifetime_collected DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
            tax_owed_lifetime_reversed DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
            tax_owed_lifetime_remitted DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
            tax_owed_updated_at DATETIME NOT NULL,
            PRIMARY KEY (tax_owed_profile_id)
        )
        """,
        cmd="post",
    )

    db.execute(
        """
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
        )
        """,
        cmd="post",
    )

    # Older installs may lack columns or indexes (quiet no-ops if already present).
    for ddl in (
        "ALTER TABLE every_circle.tax_owed "
        "ADD COLUMN tax_owed_lifetime_remitted DECIMAL(18,4) NOT NULL DEFAULT 0.0000",
        "ALTER TABLE every_circle.tax_transactions "
        "ADD COLUMN tt_tax_rate DECIMAL(18,6) NULL",
        "ALTER TABLE every_circle.tax_transactions "
        "ADD COLUMN tt_note VARCHAR(512) NULL",
        "ALTER TABLE every_circle.tax_transactions "
        "ADD UNIQUE KEY uq_tt_idempotency (tt_idempotency_key)",
        "ALTER TABLE every_circle.tax_transactions "
        "ADD KEY idx_tt_profile_id (tt_profile_id)",
        "ALTER TABLE every_circle.tax_transactions "
        "ADD KEY idx_tt_transaction_id (tt_transaction_id)",
        "ALTER TABLE every_circle.tax_transactions "
        "ADD KEY idx_tt_ti_id (tt_ti_id)",
    ):
        db.execute(ddl, cmd="post")

    _ensure_tax_transaction_uid_procedure(db)
    _TAX_OWED_TABLES_READY = True


def _ensure_tax_transaction_uid_procedure(db):
    """Create new_tax_transaction_uid if missing (idempotent)."""
    exists = db.execute(
        """
        SELECT ROUTINE_NAME
        FROM information_schema.ROUTINES
        WHERE ROUTINE_SCHEMA = 'every_circle'
          AND ROUTINE_TYPE = 'PROCEDURE'
          AND ROUTINE_NAME = 'new_tax_transaction_uid'
        """
    )
    rows = exists.get("result") or []
    if rows:
        return
    db.execute(_NEW_TAX_TRANSACTION_UID_PROCEDURE, cmd="post")


def new_tax_transaction_uid(db):
    """Allocate tt_uid via every_circle.new_tax_transaction_uid."""
    ensure_tax_owed_tables(db)
    uid_result = db.execute("CALL every_circle.new_tax_transaction_uid()")
    if not uid_result or "result" not in uid_result or not uid_result["result"]:
        return None
    return uid_result["result"][0].get("new_id")
