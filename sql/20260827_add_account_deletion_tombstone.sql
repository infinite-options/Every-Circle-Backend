-- Account deletion tombstones: anonymized profile_personal rows after users row
-- is hard-deleted; wallet frozen for audit retention (not spendable).

ALTER TABLE every_circle.profile_personal
  ADD COLUMN profile_personal_is_deleted TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN profile_personal_deleted_at DATETIME NULL,
  ADD INDEX idx_profile_personal_is_deleted (profile_personal_is_deleted);

-- Tombstones survive after the auth users row is deleted.
ALTER TABLE every_circle.profile_personal
  MODIFY COLUMN profile_personal_user_id VARCHAR(64) NULL;

ALTER TABLE every_circle.wallet
  ADD COLUMN wallet_is_frozen TINYINT(1) NOT NULL DEFAULT 0;

-- Ops audit log (no PII).
CREATE TABLE IF NOT EXISTS every_circle.account_deletion_log (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_uid VARCHAR(64) NOT NULL,
  profile_personal_uid VARCHAR(64) NOT NULL,
  deleted_at DATETIME NOT NULL,
  user_email_id VARCHAR(255) NULL,
  user_social_id VARCHAR(255) NULL,
  INDEX idx_account_deletion_log_email (user_email_id),
  INDEX idx_account_deletion_log_social (user_social_id)
);
