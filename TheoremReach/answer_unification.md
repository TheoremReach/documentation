# Answer Unification System

## Overview

The Answer Unification system clusters semantically equivalent survey answers provided by different market researchers. This enables users who have answered one version of a question to automatically qualify for surveys targeting equivalent versions without re-answering.

The primary goals of the framework are:
1. **Reduce Question Fatigue**: Users are frequently asked slight variations (or even identical versions) of the same question by different researchers. By unifying these answers, we can suppress redundant questions so a user doesn't need to answer "What is your gender?" multiple times for different providers.
2. **Improve Survey Matching**: By automatically expanding a user's profile to include all semantically equivalent answers, we significantly increase the number of surveys they qualify for.
3. **Rapid Supply Onboarding**: New market researchers can be integrated immediately. Their questions are automatically mapped to existing clusters via embeddings, giving them instant access to our demand liquidity without manual mapping.

### Strategic Benefits

This framework acts as a "Universal Translator" for survey data, offering leverage beyond the primary goals:

*   **Data Integrity & Fraud Detection (Consistency Audits)**: By knowing which questions are equivalent, we can detect users who answer inconsistently (e.g., claiming to be "Male" to one provider and "Female" to another). These **Intra-Cluster Conflicts** serve as high-confidence fraud signals.
*   **Market Intelligence**: The clusters reveal a "Standard Taxonomy" of the survey industry. We can identify gaps in provider questionnaires and potentially offer "TheoremReach Canonical Attributes" to buyers.
*   **Internationalization & Translation QA**: Clusters link equivalent concepts across languages. This allows for **Translation QA** (flagging semantic mismatches in translations) and **Cross-Lingual Inference** (inferring attributes across languages).
*   **"Orphan" Analytics**: High rates of orphaned questions (those that don't cluster) can identify providers asking irrelevant or poorly phrased questions, offering a metric for **Supply Quality**.

**Key Features:**
- **Locale-aware clustering**: Clusters are scoped per-locale (`new_country_id` + `language_id`)
- **Virtual locations**: City/State/County facets generated from MaxMind GeoIP data
- **Dynamic Read-Time expansion**: User answers are expanded via Redis at query time
- **Source-only storage**: `AppuserDemographic` stores only direct answers, not expansions
- **Deactivation pruning**: Deactivated answers are excluded from profiles

## Architecture: Read-Time Expansion

The system employs a **Read-Time Expansion** strategy to broaden targeting reach without bloating the database or search index.

### 1. Source of Truth (PostgreSQL/Mongo)
- **Source Answers Only**: Users' profiles (`AppuserDemographic`) store only the specific answers they provided (e.g., "Seattle" = 100).
- **No Expansion on Write**: We do NOT store inferred/expanded answers in the database.
- **Benefits**:
    - Optimizes storage (O(1) per answer).
    - Eliminates write amplification.
    - Simplifies data management (deletions/updates are straightforward).

### 2. Answer Unification Service (Redis)
- **Efficient Caching**: Redis stores the relationships between Facet Values and Clusters.
- **Partitioned Strategy**:
    - `fv2c:{locale}:{fvid}` -> `cluster_id` (Lookup Cluster)
    - `c2fids:{locale}:{cid}` -> Set of `facet_ids` (What questions are in this cluster?)
    - `c2fvs:{locale}:{cid}:{fid}` -> Set of `facet_value_ids` (What are the answers for this question?)
- **Performance**:
    - O(1) lookup for cluster membership.
    - Partitioning prevents fetching massive sets during expansion.
    - Supports large clusters (60k+ members) via efficient Redis Sets.

### 3. Dynamic Expansion (Read-Time)
- **`Appusers::FacetHash`**: The core service for reading user profiles.
    - Checks Redis for expansions.
    - Returns the **Source Answer + All Equivalent Answers**.
    - Used by targeting logic (`FacetsRequired`) to prevent re-asking questions.
- **ElasticSearch (Query-Time)**:
    - **Index**: Stores *Source Answers Only* to keep indices small.
    - **Query**: `Appusers::FindByFacets` expands query terms using **Redis Pipelining** (2-stage) before searching.
    - **Batching**: Large expansions (>50k IDs) are batched using `bool params` to bypass ElasticSearch term limits.
    - **Optimization**: Both `FacetHash` and `FindByFacets` use 2-stage Pipelining to resolve expansions in exactly 2 network round-trips regardless of cluster size.

### 4. Source-Only Mode (Bypass)
For consumers that only require explicit user answers (e.g., Auditing, Admin UI, raw attribute exports), `FacetHash` supports a bypass mode.
- **`source_only: true`**: Completely skips Redis expansion. Returns only the data stored in PostgreSQL/Mongo (`AppuserDemographic`).
- **Benefits**:
    - Zero Redis I/O.
    - Zero expansion overhead.
    - Guarantees 1:1 reflection of raw user input (crucial for support and auditing).

### 5. Targeted Expansion (Optimization)
To minimize Redis I/O and Ruby processing, consumers can explicitly limit the scope of expansion to specific facet IDs.
- **`target_facet_ids`**: An optional array of Facet IDs. If provided, expansion is ONLY performed for these facets.
- **`strict_mode`**: A safety flag that causes `FacetHash` to raise an `ArgumentError` if code attempts to access a facet that was not targeted.
- **Benefits**:
    - **Reduced Latency**: Skips `MGET` calls for irrelevant facets.
    - **Lower Memory**: Resulting hash contains only the data needed for the specific context.
    - **Contexts**: Used in `validate_gender!`, `SyncFacetsFromParams`, and batch-processing in `GetCampaigns`.

### 6. Semantic Clustering (Offline)
- **`unify_answers.py`**: Python script that uses embeddings (MPNet) and LLM validation (GPT-4) to group semantically equivalent answers into clusters.
- **Output**: Generates a Flat Map (`id` -> `cluster_id`) used to seed Redis.

---

## Data Flow

1. **User Answers**: User selects "Seattle" (ID 100).
2. **Write**: ID 100 is saved to `AppuserDemographic`.
3. **Targeting Check**:
    - System asks `FacetHash`: "What has this user answered?"
    - `FacetHash` checks Redis: "ID 100 belongs to Cluster A".
    - Redis returns Cluster A members: {100 (Seattle), 101 (Seattle, WA), 102 (Seattle Metro)}.
    - System sees the user has "answered" 100, 101, and 102.
4. **Qualification**: User qualifies for surveys targeting any of those IDs.

## Components

| Component | Location | Purpose |
|-----------|----------|---------|
| `AnswerUnificationWorker` | `app/workers/answer_unification_worker.rb` | Orchestrates sync process + Redis cache |
| `FacetHash` | `app/use_cases/appusers/facet_hash.rb` | **Core Reader** - expands answers via Redis |
| `FastSet` | `app/models/fast_set.rb` | O(1) Set wrapper for intersections |
| `RefreshDatabasesAnswerUnificationWorker` | `app/workers/geoip_workers/` | Updates GeoIP DB and virtual locations |
| `GenerateVirtualLocations` | `app/services/answer_unification/` | Creates city/state/county facets from MaxMind |
| `BroadcastLocation` | `app/use_cases/appusers/broadcast_location.rb` | Matches user IP location to virtual facets |
| `ChangeTracker` | `app/services/answer_unification/change_tracker.rb` | Tracks dirty FacetValue IDs |
| `unify_answers.py` | `scripts/unify_answers.py` | Core clustering algorithm |

## Suppression Safety

Cross-facet expansion must prevent "over-suppression" where a user's answer inappropriately suppresses questions they should still be asked.

### The "Non-binary" Problem

**Example**: User answers Gender Q1 with "Male" (single-select). Q1 is clustered with Q2 (multi-select with Male, Female, Non-binary). Without safeguards:
- System would expand to Q2 and mark it as "answered"
- User loses ability to answer "Non-binary" on Q2
- **Result**: User's profile is incomplete, targeting suffers

### Safety Rules

| Rule | Logic | Protects Against |
|------|-------|------------------|
| **Source Priority** | Target facet is already in user's profile | **Intra-Facet Integrity**: Prevents corrupting single-select answers or overriding explicit user choices with inferred ones |
| **Single→Multi** | If user has **only** single-select sources in a cluster, **EXCLUDE** expansion to multi-select targets. If user has **any** multi-select source, allow expansion to all target types. | User losing ability to select multiple options (while allowing expansion when user has already committed to multi-select) |
| **Coverage Check** | If target facet has values NOT in the cluster (`fc=0`), **EXCLUDE** expansion | Over-suppression of unique answer options |

### Skip Propagation

When a user skips a question (`facet_value_id = -1`, "Doesn't apply to me"):
1. `FacetHash` looks up `f2c:{prefix}:{facet_id}` to get ALL clusters for that facet
2. ALL facets in those clusters receive the skip value (-1)
3. **Skips BYPASS all safety rules** - if user says "doesn't apply", equivalent questions are also skipped

> [!IMPORTANT]
> Skips use the `f2c` key (facet → all clusters) instead of `fv2c` (value → cluster) because value `-1` doesn't have its own cluster membership.

## Virtual Location Facets

Virtual locations enable location-based targeting using MaxMind GeoIP data:

| Facet Constant | ID | Purpose |
|----------------|-----|---------|
| `FACET_ID_VIRTUAL_CITY` | 1,000,000,001 | City names (e.g., "Los Angeles") |
| `FACET_ID_VIRTUAL_STATE` | 1,000,000,002 | State/Region (e.g., "California") |
| `FACET_ID_VIRTUAL_COUNTY` | 1,000,000,003 | County/District (e.g., "Los Angeles County") |

**Key Points:**
- Virtual locations are **standard FacetValues** linked via `FacetValueMap`
- Scoped to specific `new_country_id` + `language_id` (no cross-locale matching)
- Generated from MaxMind's `GeoLite2-City-Locations-{lang}.csv`
- **Dual-Language Support**:
    - Stores both English (Canonical) and Native language text for every location.
    - Uses MaxMind native files where available (de, es, fr, ja, pt, ru, zh).
    - Uses **OpenAI (gpt-4o-mini)** to generate native translations for other locales (e.g., ko, ar), with geographic context awareness.
    - Results are cached in `#{ANSWER_UNIFICATION_CACHE_PATH}/#{locale}/translations.json`.
    - **Retry Logic**: 3 retries with exponential backoff (1s, 2s, 4s). After 5 consecutive failures, alerts via Rollbar and aborts remaining translations.
    - **Answer Matching**: The Python script preserves ALL text variations per FacetValue ID, enabling matches via any shared text (e.g., "Seoul" ↔ "서울").
    - **Question Clustering**: The export query includes a UNION to emit both native AND English question texts for virtual locations, enabling cross-language question clustering. Question texts are defined in `GenerateVirtualLocations::QUESTION_TEXTS`.

## Excluded Facets

The following facets are **excluded** from answer unification:

| Facet | ID(s) | Reason |
|-------|-------|--------|
| Exposure Tracker | 126700, 126701, 126745, 127513 | Internal tracking metadata |
| Postal facets | `postal = true` | Location handled by `SetUpLocationFromPostalCode` |
| Numeric facets | `profiler_answer_type = 3` | Not suitable for text-based clustering |

## Data Model

### AnswerCluster

```ruby
# Unique constraint: (facet_value_id, new_country_id, language_id)
AnswerCluster:
  - facet_value_id: Integer
  - cluster_id: Integer
  - new_country_id: Integer  # Locale component
  - language_id: Integer     # Locale component
```

Clusters are **isolated per locale**. The same `facet_value_id` can have different cluster relationships in different locales.

### Redis Cache Keys

Expansion data is cached in Redis for O(1) read-time lookups with suppression safety metadata:

| Key | Type | Purpose |
|-----|------|---------|
| `fv2c:{cid}:{lid}:{fvid}` | JSON | FacetValueID → `{"c":"cluster_id","t":"s"|"m"}` (cluster + type) |
| `f2c:{cid}:{lid}:{fid}` | JSON | FacetID → `{"c":["cid1",...],"t":"s"|"m"}` (all clusters for skip propagation) |
| `c2fids:{cid}:{lid}:{cluster_id}` | JSON | ClusterID → FacetIDs (JSON Array) |
| `c2fvs:{cid}:{lid}:{cluster_id}:{fid}` | JSON | Cluster+Facet → `{"v":[fvid,...],"t":"s"|"m","fc":0|1}` |

**Field Legend:**
- `t`: Source type (`s` = single-select, `m` = multi-select)
- `fc`: Fully covered flag (`1` = all facet values are in cluster, `0` = some values excluded)

> [!NOTE]
> We use **Packed JSON Strings** instead of Redis Sets (`SMEMBERS`) to allow fetching data for 100+ clusters in a single `MGET` command. This reduces the Redis Ops/Request from ~120 to ~3.

**Population**: `AnswerUnificationWorker.populate_redis_cache` populates/refreshes keys using MULTI/EXEC for atomicity.
**Fallback**: If Redis unavailable, `FacetHash` returns source-only answers (safe degradation).



## Deployment

### First-Time Setup
```ruby
# 1. Update virtual locations
GeoipWorkers::RefreshDatabasesAnswerUnificationWorker.new.perform

# 2. Dry run to preview (processes all locales)
AnswerUnificationWorker.new.perform(full_resync: true, dry_run: true)

# 3. Review output in CACHE_ROOT/debug/

# 4. Full resync (live mode)
AnswerUnificationWorker.new.perform(full_resync: true, dry_run: false)
```

### Pilot Runs
Before running a full sync, it is recommended to pilot a single locale to verify output quality:
```ruby
# Verification run for Romanian (ro-RO)
AnswerUnificationWorker.new.perform(full_resync: true, dry_run: false, locales_filter: 'ro-RO')

# Question Classification Trial Run (fast check)
AnswerUnificationWorker.new.perform(full_resync: true, dry_run: false, locales_filter: 'ro-RO', stop_before_llm_validation: true)
```

### After Migration
When migrating to locale-aware clusters, run a **full resync** to populate clusters per-locale:
```ruby
AnswerUnificationWorker.perform_async(full_resync: true)
```

### Monitoring
Check Rollbar for:
- `Answer Unification Import` - Cluster metrics per run
- `Answer Unification: Large cluster using string search` - Clusters exceeding threshold
- `FacetHash Redis expansion failed` - Redis fallback to source-only
- `Missing FacetMap for Virtual *` - Infrastructure failure

## Configuration

### Environment Variables
| Variable | Default | Purpose |
|----------|---------|---------|
| `OPENAI_ANSWER_UNIFICATION_API_KEY` | - | Enables LLM validation and Virtual Location translations |
| `ANSWER_UNIFICATION_CACHE_PATH` | `/answer_unification_cache` | Persistent cache directory |

### Audit Logging
During every production run (where `dry_run` is false), the system automatically generates and preserves human-readable audit logs in the debug directory:
- `accepted_[LOCALE].csv`: Shows every match committed to the database with similarity scores and verdicts. Always created (header-only if empty).
- `rejected_[LOCALE].csv`: Shows every pair blocked by structural guards or the LLM. Always created (header-only if empty).
- `candidates_[LOCALE].csv`: Shows ALL candidate pairs considered by the script before Phase 3 validation. Always preserved in both dry and live runs for end-to-end auditability. Always created (header-only if empty).
- `question_clusters_[LOCALE].csv`: Shows the *final* effective clusters after splitting.
    - Columns: `original_q_cluster` (semantic group), `q_cluster` (split group), `is_location`, `question_text`.
- `question_clusters_pre_classification_[LOCALE].csv`: Shows raw semantic clusters *before* splitting.
- `question_classifications_[LOCALE].csv`: Shows LLM classification of questions (Location vs Other).
- `audited_evictions_[LOCALE].csv`: Records questions evicted during Phase 1.5 audit (large cluster validation) due to semantic mismatch. Evicted answer IDs are automatically added to `orphan_ids.txt` to ensure proper tracking.
    - Columns: `timestamp`, `cluster_id`, `question_text`, `representative_text`, `reason`.
- `audit_errors_[LOCALE].csv`: Records LLM API errors or parse failures encountered during Phase 1.5 audit.
    - Columns: `timestamp`, `cluster_id`, `representative_text`, `question_text`, `error_type`, `raw_content_sample`, `error_details`.
- `[output_file].orphan_ids.txt`: Generated when questions are auto-blacklisted and the retry loop reaches max retries. This file contains the `internal_answer_id`s of orphaned records, enabling the Ruby worker to report them back to the TheoremReach platform for re-processing.

These logs are stored under `CACHE_ROOT/debug/[TIMESTAMP]/` for permanent record keeping.

### Hybrid String Search

For large clusters (>20,000 unique answers), the system switches from embedding similarity to string distance matching using `rapidfuzz`. This optimizes performance for true location clusters while preserving semantic matching for categorical questions.

| CLI Flag | Default | Purpose |
|----------|---------|----------|
| `--cache-db` | - | Path to SQLite cache database for embeddings, clusters, and LLM results |
| `--string-search-threshold` | 20000 | Threshold to switch to string search |
| `--stop-before-llm-validation` | - | Exit script immediately after Question Classification and Answer Candidate collection (Phase 2), but before LLM validation (Phase 3). |
| `--dump-question-classifications` | - | Path to CSV for question classifications debug output |
| `--detect-cached-orphans` | - | Enable proactive detection and eviction of cached orphans; see Orphan Handling (Auto-Blacklist) section. |
| `--relaxed-guards` | - | Relax safety guards (Subset, Numeric Weak, Structure). Default matches LLM mode. |
| `--max-retries` | 3 | Max retry attempts for LLM rate limits or orphan handling retry loop |

**Threshold rationale:**
- Non-location clusters (industry, occupation, workplace) max out around 8-18K unique texts
- Location clusters (cities, counties) typically exceed 20K
- The 20K threshold ensures semantic matching for non-location questions

**Memory-safe chunked processing:**
- **Input Loading:** Only required columns are loaded (`usecols`) with `low_memory=True` and explicit `dtype` to minimize DataFrame size.
- **Clusters:** Clusters exceeding 10,000 unique texts are processed in 10K×10K chunks to keep memory bounded.
- **Embeddings:** `radius_neighbors` queries (for both linking and clustering) are batched (default 1000 items) to prevent O(N*M) memory explosion.

**Rollbar alerting:** When clusters exceed the threshold, a `STRING_SEARCH_ALERT` warning is logged and forwarded to Rollbar for review.

**Example clusters:**
- Town/City (30k+) → String search
- County (93k) → String search
- Industry (~4k) → Embedding + LLM
- Workplace types (~18k) → Embedding + LLM

### Conservative Normalization

To ensure consistent matching across varied inputs while preventing data destruction, the system applies valid normalization heuristics:

**1. Leading Index Stripping**
Handles survey answers that differ only by enumeration (e.g., "1. Yes" vs "Yes").
*   **Logic**: Uses a regex `^\s*[(\（\[［]?\d+\s*(([.\-:．：－–—\u2212])(?!\s*\d)|[\)）\]］])[.\s]*` to identify indices.
*   **Safety**:
    *   **XOR Guard**: Stripping is ONLY applied if exactly one side has an index. If both have indices ("1. Yes" vs "2. Yes"), they are preserved to prevent merging distinct options.
    *   **Meaningful Remainder**: Stripped text must be ≥ 2 characters.
    *   **Numeric Protection**: Negative lookahead `(?!\s*\d)` prevents stripping decimals, ranges, or times (e.g., "3.5mm", "1 - 5", "１．５", "１：３０", "1−5" are NOT stripped).

**2. Complex Script Handling (Unicode Category Filtering)**
For CJK and Complex Scripts (Arabic, Hebrew, Thai, Indic), standard "romanization" (unidecode) is dangerous because it causes collisions (e.g., "Fukuoka" and "Tomioka" sharing a romanization) or destroys meaning (stripping Thai tone marks).

*   **Complex Scripts** (Arabic, Hebrew, Thai, Indic, Khmer, Ethiopic):
    *   **Action**: Skips `unidecode`. Instead, strips only **Punctuation (P*)** and **Symbols (S*)** using Unicode categories, while preserving **Letters (L*)** and **Marks (M*)**.
    *   **Benefit**: Preserves tone marks and vowels that are critical for meaning in Thai/Devanagari, while still allowing "fuzzy" punctuation matching (e.g., "สวัสดี." matches "สวัสดี").
*   **CJK** (Chinese, Japanese, Korean):
    *   **Action**: Skips `unidecode`. Uses `rapidfuzz.default_process` (if available) or simple whitespace stripping.
*   **Latin/Cyrillic/Greek**:
    *   **Action**: Uses standard `unidecode` + `rapidfuzz` normalization for maximum matching power (e.g., "Zürich" -> "zurich").

### LLM Question Classification

To prevent false positives in high-volume "location list" questions (where determining equivalence between distinct entities like "County A" and "County B" is prone to LLM hallucination), the system classifies questions before processing.

*   **Logic**:
    *   **YES (Location)**: "Does this question ask for a specific geographic location (City, County)?" -> **Force String Search Mode** (still validated by LLM).
    *   **NO (Other)**: All other questions (Job Title, Industry, Demographics). -> **Standard** (Embeddings or String Search depending on size).
*   **CLI Flag**: `--classify-questions` (enabled by default). Use `--no-classify-questions` to disable.
*   **Result**: Location questions use `rapidfuzz` string matching regardless of cluster size, optimized for finding candidates in large lists, then verified by LLM.

### Cluster Splitting (Algorithm Downgrade Prevention)

To prevent "algorithm downgrading" where a single Location question (YES) mistakenly clustered with a Standard question (NO) forces the entire cluster into String Search mode (losing semantic matching capabilities for the Standard question), the system performs **Post-Classification Splitting**:

1.  **Grouping**: Questions are first clustered semantically (Phase 1).
2.  **Splitting**: If a semantic cluster contains both YES and NO questions, it is split into two distinct sub-clusters:
    *   `{ClusterID}_LOC`: Contains all Location questions. Uses Forced String Search.
    *   `{ClusterID}_STD`: Contains all Standard questions. Uses Embeddings (unless cluster size is huge).
3.  **Result**: Each sub-cluster uses the optimal matching strategy for its content type, while preserving the original semantic grouping for audit purposes (via `original_q_cluster` column).

### Safety Guards
- **Single-Question Skip**: Clusters containing only one unique question are SKIPPED. This avoids merging "intra-question" variations (e.g., "Good" vs "Great") if they are distinct within that question.
- **Co-occurrence Guard**: Even in multi-question clusters, answers from the *same* Question ID are NEVER merged, even if they have identical text. This ensures distinct options (e.g., "Yes" (101) vs "Yes" (102) if duplicate) remain separate.
- **Cold Start Guard**: Incremental syncs blocked until full resync completes
- **Lock**: Prevents concurrent runs
- **Batch Size**: 1000 entries per transaction
- **Locale Isolation**: Clusters cannot leak between locales

### Orphan Handling (Auto-Blacklist)

When incremental updates add new questions, they are linked to existing clusters based on embedding similarity. Occasionally, a question is incorrectly linked (e.g., "Dog breeds" linked to a "Car models" cluster). The LLM validation correctly rejects these matches, but the question becomes "orphaned"—stuck in a bad cluster with no valid peers.

**Auto-Blacklist Mechanism:**
1. After Phase 3 validation, the system identifies questions that:
   - Were linked to an existing cluster
   - Had ≥1 candidate pairs tested
   - Had 0 successful matches

2. These questions are automatically:
   - Blacklisted from the failed cluster (stored in `cluster_exclusions` table)
   - Evicted from the `question_clusters` cache
   - Logged with a warning: "Auto-blacklisting question..."

3. An **immediate retry** attempts to re-cluster the orphan:
   - The blacklist prevents re-linking to the bad cluster
   - The question finds its next-best match or forms a new cluster
   - If successful, the orphan is resolved in the same run

4. If retry fails (LLM errors, cost limits, or no valid matches):
   - Orphan answer IDs are written to `{output}.orphan_ids.txt`
   - The worker keeps these IDs dirty for the next run
   - On subsequent runs, the blacklist prevents repeating the bad link

**Proactive Orphan Detection (Full Resync Only):**

During a `full_resync`, the system also performs **proactive detection** of questions that were orphaned in previous runs but are still cached:

1. Before Phase 1 clustering, the system scans the `llm_decisions` cache
2. Questions with ALL cached decisions rejected are identified as "cached orphans"
3. These questions are evicted from `question_clusters` and blacklisted
4. They are then re-clustered in the normal Phase 1 flow

This makes full resyncs **self-healing**—they clean up historical orphans automatically.

### Orphan Tracking Database

In addition to the auto-blacklist mechanism, the system maintains a persistent record of orphans in the `answer_unification_orphans` table for admin review and debugging.

**Schema:**
- `facet_id`: The question ID (for high-level grouping).
- `facet_value_id`: The specific answer ID that failed clustering.
- `new_country_id` / `language_id`: Locale context.
- `cluster_failure_reason`: Why it failed (e.g., `no_candidates`, `llm_rejection`).

**Management Rake Tasks:**
- `rake answer_unification:backfill_orphans_db[locale]`: Backfills the table by comparing active FacetValues vs. AnswerClusters.
- `rake answer_unification:retry_orphans[locale]`: Marks orphaned IDs as "dirty" in `ChangeTracker` so they are retried in the next worker run.

**CLI Flag**: `--detect-cached-orphans` (default: enabled on full_resync)

**Standard Facet Flagship Priority:**

Standard facets (category 1, e.g., Education, Income, Occupation) receive special protection to ensure they anchor their clusters:

1. **Flagship Status**: Standard facet questions are always marked as flagships
2. **No Inbound Linking**: Standard facets never link to existing non-standard clusters
3. **Blacklist Protection**: Standard facets are never blacklisted even if validation fails
4. **Attract Non-Standard**: Non-standard questions CAN link to standard facet clusters

This ensures that well-curated standard facet question texts remain the canonical center of their clusters, preventing "drift" where a standard facet could be absorbed into a cluster formed by ad-hoc campaign provider questions.

**CLI Flag**: `--protect-standard-facets` (default: enabled)

### Dynamic Cluster Auditing (Phase 1.5)

Large clusters are vulnerable to becoming "Garbage Bins" where semantically unrelated questions accumulate due to generic answer text (e.g., "None of the above"). **Dynamic Cluster Auditing** validates large clusters via LLM before answer matching.

**Process:**
1. After Phase 1 clustering, clusters exceeding `AUDIT_MIN_CLUSTER_SIZE` (default: 10) are audited
2. A **representative** is elected for each cluster:
   - If cluster contains a Standard Facet (category=1), it becomes the representative
   - Otherwise, the **medoid** (question closest to cluster centroid) is elected
3. All other cluster members are validated against the representative using a **Cluster Audit** LLM prompt
4. Questions failing validation are **evicted** to `cluster_exclusions` and trigger a retry
5. **Orphan Tracking**: Evicted answer IDs are added to `all_orphan_answer_ids` and written to `orphan_ids.txt`, ensuring they are persisted to `AnswerUnificationOrphan` for monitoring
6. The retry loop re-clusters evicted questions with their blacklist in place
7. **Efficiency**: Answer samples are generated only for the subset of questions involved in the audit, ensuring performance scales with audit size ($O(N_{audited})$) rather than total dataset size ($O(N_{total})$).
8. **Cost Tracking**: LLM token usage and estimated costs are logged at the completion of the audit phase.

**CLI Flags:**
| Flag | Default | Purpose |
|------|---------|---------|
| `--no-audit` | false | Disable Phase 1.5 auditing |
| `--audit-min-cluster-size` | 10 | Minimum cluster size to trigger audit |
| `--dump-audited-evictions` | - | Path to CSV for eviction records |
| `--dump-audit-errors` | - | Path to CSV for audit error records |

**Configuration (Environment Variables):**
- `AUDIT_MIN_CLUSTER_SIZE`: Override default cluster size threshold
- `AUDIT_LLM_BATCH_SIZE`: Questions per LLM batch (default: 20)
- `AUDIT_INCREMENTAL`: If `true`, only audit clusters containing dirty IDs

### Retry Loop Termination Strategy

Both the audit loop (Phase 1.5) and orphan loop (Phase 3.B) use **threshold-based termination** to balance convergence quality against runtime/cost.

**Termination Hierarchy** (in order of precedence):

1. **Zero count**: If evictions/orphans reach 0, loop exits immediately (fully stabilized)
2. **Absolute threshold**: If count < threshold, loop exits (problem is small enough)
3. **Improvement threshold**: If improvement < X% from previous attempt, loop exits (diminishing returns)
4. **Max retries**: Per-phase hard cap to prevent infinite loops
5. **Global safety cap**: Maximum total loop iterations (default: 15)

**Default Configuration:**

| Parameter | Audit Loop | Orphan Loop |
|-----------|------------|-------------|
| Absolute threshold | 500 | 200 |
| Improvement threshold | 10% | 10% |
| Max retries | 5 | 5 |

**CLI Overrides:**

| Flag | Purpose |
|------|---------|
| `--audit-termination-threshold N` | Stop audit loop if evictions below N |
| `--audit-improvement-threshold X` | Stop audit loop if improvement below X (0.0-1.0) |
| `--orphan-termination-threshold N` | Stop orphan loop if orphans below N |
| `--orphan-improvement-threshold X` | Stop orphan loop if improvement below X (0.0-1.0) |
| `--max-audit-retries N` | Max retries triggered by audit evictions |
| `--max-orphan-retries N` | Max retries triggered by orphan detection |

**Observability:**

The retry loop logs metrics for each iteration:
```
audit.retry_metrics loop=0 audit_retries=0 evictions=3297 prev=None improvement=0.0%
Phase 1.5: Audit evicted 3297 questions (0.0% improvement). Triggering retry (Audit Retry 1/5)...
audit.retry_metrics loop=1 audit_retries=1 evictions=2349 prev=3297 improvement=28.8%
```

Termination reason is also logged:
```
Phase 1.5: Audit stabilized (improvement (8.2%) below threshold (10%)). Proceeding to Phase 2.
```

**Worker Ghost Key Cleanup:**

The `AnswerUnificationWorker` includes automatic cleanup of stale Redis keys:
- **`seed_cache`**: "Mark and Sweep" - deletes fv2c keys not in current clusters
- **`update_cache_incremental`**: "Track and Purge" - removes orphaned fv2c keys
- **`reset_question_clusters(clear_redis: true)`**: Clears all fv2c/c2fvs/c2fids keys

## Troubleshooting

### Force Re-audit All Users
```ruby
AnswerUnification::AuditAppuser.batch_audit(Appuser.ids, batch_size: 500)
```

### Verify Locale Isolation
```ruby
# Check if a facet_value_id has clusters in multiple locales
AnswerCluster.where(facet_value_id: 100).pluck(:new_country_id, :language_id)
```

### Regenerate Virtual Locations for a Single Locale
```ruby
AnswerUnification::GenerateVirtualLocations.new(locale: 'en-US').execute
```

### Run Database Maintenance
```ruby
AnswerUnificationWorker.run_maintenance  # Vacuums SQLite cache DBs
```

### Reset Question Clusters Cache
Forces all questions to be re-clustered on next full_resync. Preserves embeddings and LLM decisions.

```ruby
# Clear question clusters only (preserves blacklist, orphans, and AnswerCluster records)
AnswerUnificationWorker.reset_question_clusters(clear_exclusions: false)

# Default: clear question clusters + blacklist + orphan tracking (recommended for fresh start)
AnswerUnificationWorker.reset_question_clusters

# Complete reset: clear SQLite cache AND PostgreSQL AnswerCluster records
AnswerUnificationWorker.reset_question_clusters(clear_answer_clusters: true)

# Nuclear option: clear everything including Redis
AnswerUnificationWorker.reset_question_clusters(clear_exclusions: true, clear_answer_clusters: true, clear_redis: true)
```

**Parameters:**
- `clear_exclusions` (default: `true`): Clears `cluster_exclusions` blacklist AND `AnswerUnificationOrphan` records
- `clear_answer_clusters` (default: `false`): Clears PostgreSQL `AnswerCluster` records (affects InputExpander)
- `clear_redis` (default: `false`): Clears all Redis fv2c/c2fvs/c2fids/f2c keys

Use when deploying new clustering logic (e.g., flagship facet protection) or fixing corrupted cluster assignments.

### Orphaned Questions Not Resolving

If a question remains orphaned across multiple runs:
1. Check `cluster_exclusions` table for excessive blacklist entries
2. Verify LLM API is responding correctly (not returning errors)
3. Consider running a full resync to trigger proactive orphan detection
4. As a last resort, clear the question_clusters cache for the affected locale

```sql
-- View blacklist entries for a question
SELECT * FROM cluster_exclusions WHERE question_text LIKE '%Dog%';

-- Clear blacklist for a specific question (use with caution)
DELETE FROM cluster_exclusions WHERE question_text = 'What breed is your dog?';

-- Check if a question is a cached orphan (all decisions rejected)
SELECT pair_key, match FROM llm_decisions WHERE pair_key LIKE '%Dog%';
```

## Unification Algorithm Logic

The `unify_answers.py` script follows a multi-phase process to safely cluster answers while handling scale and semantic nuance.

```mermaid
flowchart TD
    subgraph Phase1["Phase 1: Question Clustering"]
        Q[Input Questions] -->|LLM Classifier| C{Is Location?}
        C -->|Yes| P1_LOC[Tag: LOCATION]
        C -->|No| P1_STD[Tag: STANDARD]
        
        P1_LOC --> SPLIT
        P1_STD --> CHECK_STD{Is Standard\nFacet?}
        CHECK_STD -->|Yes| ANCHOR[Anchor New Cluster]
        CHECK_STD -->|No| SPLIT[Semantic Linking]
        ANCHOR --> SPLIT
        
        SPLIT -->|Mixed Cluster?| SPLIT_LOGIC{Contains<br/>Loc & Std?}
        SPLIT_LOGIC -->|Yes| S_SPLIT[Split into<br/>_LOC and _STD]
        SPLIT_LOGIC -->|No| S_KEEP[Keep Original]
    end

    subgraph Phase2["Phase 2: Candidate Generation"]
        S_SPLIT --> MODE_CHECK{Cluster Type?}
        S_KEEP --> MODE_CHECK
        
        MODE_CHECK -->|Location / Huge| M_STR[String Search Mode]
        MODE_CHECK -->|Standard| M_EMB[Embedding Mode]
        
        subgraph StringSearch["Rapidfuzz (Levenshtein)"]
            M_STR --> R_TOK[Token Sort Ratio]
            R_TOK -->|Sim > 90%| GUARDS
        end
        
        subgraph Embeddings["Cosine Similarity"]
            M_EMB --> E_STAR{Star Topology?}
            E_STAR -->|Yes| E_FLAG[Compare only\nvs Flagship]
            E_STAR -->|No| E_ALL[Compare All\nN*N]
            E_FLAG --> E_SIM[Dot Product]
            E_ALL --> E_SIM
            E_SIM -->|Sim > 0.80| GUARDS
        end
        
        subgraph SafetyGuards["Safety Guards"]
            GUARDS[Candidate Pair] --> G_NUM{Numeric<br/>Mismatch?}
            G_NUM -->|Fail| REJECT_G[Reject]
            G_NUM -->|Pass| G_SUB{Subset<br/>Mismatch?}
            G_SUB -->|Fail| REJECT_G
            G_SUB -->|Pass| G_STR{Structure<br/>Mismatch?}
            G_STR -->|Fail| REJECT_G
            G_STR -->|Pass| G_DATE{Date<br/>Mismatch?}
            G_DATE -->|Fail| REJECT_G
            G_DATE -->|Pass| CANDIDATES[Valid Candidate]
        end
    end

    subgraph Phase3["Phase 3: LLM Validation"]
        CANDIDATES -->|Batch| LLM[GPT-4o-mini]
        LLM -->|Prompt| PROMPT["Q: What is your gender?\nA: Male\nQ: What's your gender?\nA: Man\n\nAre these equivalent?"]
        PROMPT -->|Response| DECISION{Verdict?}
        
        DECISION -->|YES| MATCH[Match & Propagate]
        DECISION -->|NO| REJECT_L[Reject & Check Orphan]
        DECISION -->|Transient Error| RETRY[Fail Locale / Retry IDs]
        
        REJECT_L --> ORPHAN{Is Orphan?}
        ORPHAN -->|Yes| BLACKLIST[Blacklist & Retry Phase 1]
    end
    
    MATCH --> DB[(AnswerCluster)]
    BLACKLIST -.->|Retry| Q
```

### Phase 1: Question Clustering
To prevent false positives between distinct entities (e.g., "Paris, TX" vs "Paris, TN"), questions are classified before processing.
- **Location Questions**: Forced into **String Search Mode**. This ensures that "County A" and "County B" are never matched solely by semantic similarity.
- **Standard Facet Protection**: Standard Facet questions (Gender, Age, etc.) are prevented from linking to existing non-standard clusters. They anchor their own clusters to ensure high quality.
- **Standard Questions**: Use **Embedding Mode** to catch semantic variations (e.g., "Software Engineer" ~= "Developer").

### Phase 2: Candidate Generation
Candidates are generated within each cluster using the appropriate strategy:
- **String Search Mode**: Uses `rapidfuzz` (Levenshtein distance). Candidates must have >90% token sort ratio.
- **Embedding Mode**: Uses cosine similarity of sentence embeddings. Candidates must have >0.80 similarity.
    - **Star Topology**: If a cluster contains a Standard Facet ("Flagship"), other questions are compared ONLY to the Flagship, reducing comparisons from O(N²) to O(N).

**Safety Guards** are applied to all candidates to prevent dangerous merges:
1.  **Numeric Guard**: Strips indices ("1. Yes") but protects values. Rejects if numbers differ (e.g., "2 years" vs "5 years").
2.  **Subset Guard**: Ensures one answer isn't a superset of another (e.g., "100g" vs "100g (Pack of 3)").
3.  **Structure Guard**: Checks for conflicting units or symbols (e.g., "$5" vs "5%").

### Phase 3: Validation
Evaluating candidates is expensive, so only survivors of Phase 2 reach the LLM.
- **Model**: `gpt-4o-mini`
- **Output**: Strict `YES` or `NO` decision.
- **Caching**: Decisions are cached in SQLite to minimize costs on future runs.
- **Auto-Blacklist & Retry**: If a question consistently fails validation against its cluster peers, it is flagged as an **Orphan**. The system automatically blacklists the bad cluster link and retries clustering (Phase 1) within the same run to find a better home.

