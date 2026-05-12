# Firestore TTL setup — `live_forecast` collection

The Spark streaming consumer (`src/streaming/spark_consumer.py`) writes
onset-alert documents into Firestore collection **`live_forecast`** with
an `expires_at` timestamp set to *now + 7 days*. This TTL policy tells
Firestore to delete those documents once `expires_at` has passed, so the
collection never grows beyond a one-week window.

> **Why this is a manual procedure.**
> Firestore TTL policies cannot be created with `gcloud` today — they
> must be configured via the Cloud Console *or* the Firestore Admin SDK
> (`google-cloud-firestore-admin`). Console steps are the lightest
> option for a one-time setup, so they are documented below.

---

## Prerequisites

- Firestore database created in **Native mode** in the project
  `climate-prediction-system` (region: `us-central1` or `nam5` — match the
  region used by the rest of the stack).
- IAM role on your user account: `Datastore Owner` (or higher).
- The streaming consumer has been run at least once so the
  `live_forecast` collection exists. (Firestore allows TTL on
  collections that don't exist yet, but the UI is easier when the
  collection is visible.)

---

## Step-by-step (Cloud Console)

1. Open <https://console.cloud.google.com/firestore/databases> and pick
   the `climate-prediction-system` project.
2. Select the `(default)` database.
3. In the left sidebar, click **Time-to-live (TTL)**.
4. Click **+ Create policy**.
5. Fill in the form:
   - **Collection group**: `live_forecast`
   - **Timestamp field**: `expires_at`
6. Click **Create**.
7. Wait ~10 minutes for the policy to enter `Active` state. The page
   refreshes automatically; the status column flips from
   `Creating` → `Active`.

That's it — Firestore now sweeps `live_forecast` continuously and
deletes any document whose `expires_at` has passed.

---

## Verification

After at least one successful streaming batch has fired:

```bash
gcloud firestore databases describe \
  --project="${GCP_PROJECT_ID}" \
  --database="(default)" \
  --format="value(state)"
```

You can also confirm the policy exists by visiting the same
**Time-to-live (TTL)** page — `live_forecast` should appear in the list
with status `Active`.

To eyeball a written doc:

```bash
gcloud firestore documents list \
  --project="${GCP_PROJECT_ID}" \
  --collection-id="live_forecast" \
  --limit=5
```

Each document should have an `expires_at` field roughly 7 days in the
future relative to its `ingested_at` field.

---

## Operational notes

- **Latency**: Firestore TTL is *eventual* — documents may live up to
  24 hours past `expires_at` before deletion. That is acceptable here
  because the dashboard always overwrites a county's doc on the next
  batch (county name is the doc id), so stale state never surfaces.
- **Cost**: TTL deletions are billed as standard document deletes, but
  the volume is bounded by `10 counties × 1 alert/hour × 24 hours = 240`
  writes/day worst-case. Negligible.
- **Removing the policy**: same TTL page → click the row → **Delete
  policy**. Documents already in the collection are *not* removed; only
  the auto-sweep stops.
- **Field type**: TTL requires the field to be a Firestore `timestamp`.
  The Python sink uses `datetime` objects, which the
  `google-cloud-firestore` client serialises as `timestamp` — no extra
  conversion needed.

---

## Alternative — Admin SDK (scripted)

If you'd rather automate this in CI later, the Firestore Admin API
endpoint is `firestore.googleapis.com/v1/projects/{project}/databases/{db}/collectionGroups/{group}/fields/{field}`
with a PATCH that sets `ttlConfig`. The corresponding Python helper is
`google.cloud.firestore_admin_v1.FirestoreAdminClient.update_field`.
Out of scope for M3 — the console procedure above is the documented
path.
