-- Restock audit tables were originally keyed one row per (trr_uid, listing) for
-- strict idempotency. Cumulative restock (SUM(quantity) <= trr_return_quantity)
-- requires multiple audit rows per return.

ALTER TABLE every_circle.profile_expertise_restocks
  DROP INDEX uq_per_trr_offering;

ALTER TABLE every_circle.business_service_restocks
  DROP INDEX uq_bsr_trr_bs;

-- Keep lookup performance for SUM(per_quantity) by return + offering.
CREATE INDEX idx_per_trr_offering
  ON every_circle.profile_expertise_restocks (per_trr_uid, per_profile_expertise_uid);

CREATE INDEX idx_bsr_trr_bs
  ON every_circle.business_service_restocks (bsr_trr_uid, bsr_bs_uid);

-- Repair known drift from verification (inventory 37, audit still showed 2).
UPDATE every_circle.profile_expertise_restocks
SET per_quantity = 3, per_remaining = 37
WHERE per_trr_uid = '540-000069'
  AND per_profile_expertise_uid = '150-000134'
  AND per_quantity = 2;
