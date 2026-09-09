-- 30-day soft-delete grace: schedule permanent purge after account deletion request.

ALTER TABLE every_circle.profile_personal
  ADD COLUMN profile_personal_purge_scheduled_at DATETIME NULL,
  ADD INDEX idx_profile_personal_purge_scheduled_at (profile_personal_purge_scheduled_at);
