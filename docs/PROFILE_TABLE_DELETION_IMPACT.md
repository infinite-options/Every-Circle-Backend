# Impact of Deleting the `profile` Table (profile_DNU)

The `every_circle.profile` table has been renamed to `profile_DNU` in the database and is not actively used. The `/profile` API endpoint and `user_profile.py` have been removed from this codebase.

The following backend code still references the **profile** table (or would reference it if the table name is still `every_circle.profile` in code). If the profile table is **deleted** (or dropped), the behavior is as follows.

---

## 1. **feed.py**

- **Reference:** `db.select('every_circle.profile', where={'profile_uid': profile_id})`
- **Endpoint:** `GET /feed/<string:profile_id>`
- **If table deleted:** Query fails (table/view missing). Feed GET returns 500 or DB error; no feed data for any profile_id.
- **Migration option:** Use `profile_personal` and `profile_personal_uid` instead, and read “how can we help” (or equivalent) from a column there if it exists; otherwise drop or repurpose the Feed feature.

---

## 2. **ec_api.py – Referral (“Refer a friend”)**

- **Reference:** `db.select('every_circle.profile', where={'profile_uid': payload['profile_uid']})`
- **Endpoint:** `POST /refer-a-friend`
- **Usage:** Validates that the referrer exists and reads `profile_first_name`, `profile_last_name` for the email body.
- **If table deleted:** Validation fails (no row found) → 404 “User does not exist”. Referral flow broken.
- **Migration option:** Validate and read names from `every_circle.profile_personal` using `profile_personal_uid` (treat payload `profile_uid` as `profile_personal_uid` if your app uses that convention).

---

## 3. **user_connections.py**

- **Reference:** Raw SQL uses unqualified table name `profile` (e.g. `FROM profile`, `FROM profile p`, `JOIN profile p`). Assumes a table named `profile` in the default schema (e.g. `every_circle.profile`).
- **Endpoint:** `GET /api/v1/connections/<string:profile_id>`
- **If table deleted:** Query fails (table missing). Connections endpoint returns DB error.
- **Migration option:** Rewrite the recursive CTE to use `profile_personal` and `profile_personal_uid` / `profile_personal_referred_by` (and adjust column names: e.g. `profile_first_name` → `profile_personal_first_name`).

---

## 4. **search.py**

- **Reference:** Multiple raw SQL blocks use `FROM profile` and `FROM profile p` in recursive referral CTEs. Also joins `ratings` with `rating_profile_id` (which may point at profile_uid or profile_personal_uid depending on app).
- **Endpoint:** `GET /search/<string:profile_id>` and `GET /api/v2/search/<string:profile_id>`
- **If table deleted:** Queries that reference `profile` fail. Search by profile/connections breaks.
- **Migration option:** Switch referral/connection logic to `profile_personal` and align `rating_profile_id` semantics with `profile_personal_uid` if that’s the intended meaning.

---

## 5. **sambanovasearch.py**

- **Reference:**
  - Recursive CTE uses `FROM profile` and `FROM profile p` for referral paths.
  - `INNER JOIN every_circle.profile p ON r.rating_profile_id = p.profile_uid` to get rater profile fields (first_name, last_name, images, etc.).
- **Endpoint:** Used by AI Direct Business Search (`/api/v1/aidirectbusinesssearch/<string:profile_id>`).
- **If table deleted:** Both the connection query and the ratings join fail. Feature breaks.
- **Migration option:** Use `profile_personal` for both referral graph and rater info; join `ratings` to `profile_personal` on `rating_profile_id = profile_personal_uid` if that’s how IDs are stored.

---

## 6. **ratings.py**

- **Reference:** `db.select('every_circle.profile', where={'profile_uid': profile_uid})` to validate that the profile exists before creating/updating a rating.
- **Endpoint:** Ratings POST/PUT (e.g. `/ratings`, `/api/v2/ratings`).
- **If table deleted:** “User does not exist” 404 when creating/updating ratings; rating creation/update broken for any flow that validates against profile.
- **Migration option:** Validate against `profile_personal` by `profile_personal_uid` (if `rating_profile_id` is effectively profile_personal_uid).

---

## 7. **ratings_v3.py**

- **Reference:** `db.select('every_circle.profile', where={'profile_uid': profile_uid})` to validate profile and to get `profile_user_id` for business creation when the business doesn’t exist.
- **Endpoint:** `POST /api/v3/ratings_v3` (and related).
- **If table deleted:** 404 “User does not exist” and inability to resolve `user_uid` for auto-creating businesses; ratings_v3 flow broken.
- **Migration option:** Use `profile_personal` to validate and to get `profile_personal_user_id` as the user_uid for business creation.

---

## 8. **data_ec.py – processImage**

- **Reference:** When `key` contains `'profile_uid'`, code runs `SELECT profile_images_url FROM every_circle.profile WHERE profile_uid = ?`.
- **Usage:** Only the removed `user_profile.py` (Profile API) called `processImage` with `profile_uid`. No remaining endpoint does.
- **If table deleted:** This branch is effectively dead code. If something were to call `processImage` with `profile_uid` again, the query would fail.
- **Migration option:** Remove the `'profile_uid' in key` branch in `processImage` to avoid future confusion, or point it at a profile_personal image column if you reintroduce a similar API.

---

## Summary Table

| File / area        | Endpoint / usage              | If profile table deleted      |
|--------------------|------------------------------|--------------------------------|
| feed.py            | GET /feed/<profile_id>       | Feed fails (DB error)          |
| ec_api.py          | POST /refer-a-friend         | Referral 404 / broken          |
| user_connections.py| GET /api/v1/connections/...   | Connections fail (DB error)    |
| search.py          | GET /search/..., /api/v2/search/... | Search/connections fail |
| sambanovasearch.py  | AI Direct Business Search    | Feature fails (DB error)       |
| ratings.py         | Ratings POST/PUT             | Rating create/update 404        |
| ratings_v3.py      | POST /api/v3/ratings_v3      | Rating create 404, no user_uid |
| data_ec.py         | processImage(profile_uid)    | Dead code; would fail if used  |

---

## Recommendation Before Dropping the Table

1. **Migrate all above usages** to `every_circle.profile_personal` (and, if needed, `profile_personal_referred_by` for referral chains). Replace `profile_uid` with `profile_personal_uid` and column names with their `profile_personal_*` equivalents where applicable.
2. **Clarify `rating_profile_id`:** Ensure it is defined and used consistently as either legacy `profile_uid` or `profile_personal_uid`; then update all joins and validations to use `profile_personal` once the profile table is gone.
3. **Remove or refactor** the `profile_uid` branch in `data_ec.processImage` so no code path hits the old table.
4. After migration and deployment, **drop the profile table** (or keep `profile_DNU` only for history/backup if desired).
