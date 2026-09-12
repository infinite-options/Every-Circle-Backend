-- Phone OTP verification: verified flag on users + hashed challenge store.

ALTER TABLE every_circle.users
  ADD COLUMN user_phone_verified TINYINT(1) NOT NULL DEFAULT 0;

CREATE TABLE every_circle.phone_otp_challenges (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_uid VARCHAR(64) NOT NULL,
  phone_e164 VARCHAR(20) NOT NULL,
  code_hash CHAR(64) NOT NULL,
  code_salt VARCHAR(64) NOT NULL,
  expires_at DATETIME NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  consumed_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_phone_otp_challenges_user_uid (user_uid),
  INDEX idx_phone_otp_challenges_lookup (user_uid, phone_e164, consumed_at, expires_at)
);
