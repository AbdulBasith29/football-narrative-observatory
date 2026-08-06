# Data Source Assessment

Can YouTube actually provide the data required to answer each research question, at sufficient quality and within quota and privacy constraints? This document evaluates the technical realities of the YouTube Data API v3 and establishes how source limitations drive our engineering constraints.

## Source Overview
- **Source name**: YouTube Data API v3
- **API or access method**: REST API (`https://www.googleapis.com/youtube/v3/`)
- **Authentication requirements**: API Key (Server-to-Server)
- **Quota model**: Separate daily limits: 10,000 general queries (for comment/video endpoints), 100 search queries, and 10,000 video batch statistics queries.
- **Historical availability**: Generally exhaustive for active videos, but subject to deletion, privacy changes, and missing historical context if a video is removed or comments are disabled.
- **Terms and policy constraints**: Strict rules against deriving personally identifiable information (PII), storing raw author identifiers indefinitely without pseudonymisation, or exposing API data in ways that violate YouTube's Developer Policies.

## Required Endpoints
- `search.list`: Used for video discovery when compiling an event catalogue (high quota cost, use sparingly).
- `videos.list`: Used for retrieving video metadata (published date, tags, category, title, description, statistics).
- `channels.list`: Used for verifying channel metadata and stratifying channels.
- `commentThreads.list`: The primary ingestion endpoint. Retrieves top-level comments and basic thread metadata for a given `videoId`.
- `comments.list`: Used for retrieving full replies to a specific parent comment.

## Available Fields
For each endpoint, the following critical fields are available:
- **identifiers**: `videoId`, `channelId`, `commentId`, `authorChannelId` (when available).
- **timestamps**: `publishedAt`, `updatedAt`.
- **text**: `textOriginal`, `textDisplay`, `snippet.title`.
- **engagement**: `likeCount`, `replyCount`, `viewCount`, `commentCount`.
- **parent-child relationships**: `parentId` (for replies).
- **channel metadata**: `subscriberCount`, `customUrl`, `defaultLanguage`.
- **pagination fields**: `nextPageToken`.

## Unsupported or Unreliable Fields
The API cannot reliably provide:
- True commenter geography or demographics.
- Complete lifetime author history (we only observe authors when they comment on our sampled videos).
- Guaranteed stable author identifiers (users can change their handles, and `authorChannelId` is sometimes omitted).
- Server-side comment `publishedAfter` filtering on `commentThreads.list`.
- Exhaustive historical comment coverage for massive viral videos (due to quota limits and API truncations).
- Complete replies directly from a single `commentThreads.list` call.

## Research Question Feasibility Matrix

| Research Question | Required Data | Source Support | Status |
|---|---|---|---|
| **RQ1** (Reaction shifts) | timestamps, targets, event links | supported with processing | `SUPPORTED` |
| **RQ2** (Tone vs mix) | channel, sentiment, volume windows | supported | `SUPPORTED` |
| **RQ3** (Author overlap) | stable author IDs across windows | partial | `PARTIALLY_SUPPORTED` |
| **RQ4** (Target treatment) | entity resolution, comment text | supported | `SUPPORTED` |
| **RQ5** (Narrative drivers) | textual claims, targets | supported | `SUPPORTED` |
| **RQ6** (Narrative lifecycle) | long-term historical coverage | limited | `DEFERRED` |
| **RQ7** (Sarcasm/Derision) | discourse text | supported | `SUPPORTED` |
| **RQ8** (Channel baselines) | versioned channel strata | supported | `SUPPORTED` |
| **RQ9** (Matched events) | structural event metadata | supported | `DEFERRED` |
| **RQ10** (Relevance vs chrono) | time and relevance retrieval modes | supported | `DEFERRED` |

## Quota Assessment

The project currently receives separate API quota buckets:
- **General API queries**: 10,000 per day
- **Search queries**: 100 per day
- **Video batch statistics queries**: 10,000 per day

`commentThreads.list`, `comments.list`, and `videos.list` generally consume one query per request from the general bucket. `search.list` is governed by the separate limit of 100 calls per day.

Because search capacity is substantially more constrained than ordinary read capacity, video discovery is performed periodically and discovered video identifiers are persisted for later ingestion.

- **Expected top-level comment cost per video per run**: Up to five general queries for the configured 500-comment cap.
- **Metadata cost**: Normally one or a small number of batched `videos.list` or `channels.list` requests.
- **Discovery cost**: Search capacity is limited to 100 `search.list` requests per day and is therefore treated as a separately budgeted, non-continuous workflow.
- **Worst-case uncapped comment scan**: A video containing 100,000 top-level comments could require roughly 1,000 paginated comment-thread requests, excluding retries and replies.

### Operational Budgets
**Search Budget (100 calls/day)**
- New event discovery: 50 calls/day
- Tracked-query refresh: 25 calls/day
- Validation/manual research: 15 calls/day
- Reserve: 10 calls/day

**General-Query Budget (10,000 calls/day)**
- Chronological comments: 60%
- Metadata refresh: 10%
- Retries: 10%
- Backfill: 15%
- Reserve: 5%
*(Note: We enforce balanced sampling counts per stratum rather than splitting the general quota permanently by orientation).*

## Sampling Implications
Because of the above limitations, our sampling protocol must enforce:
- **Chronological page scanning**: We must paginate backward in time (`order=time`) to capture temporal windows.
- **Composite watermarks**: Because there is no `publishedAfter` filter, we must store the latest observed timestamp and associated comment IDs to know when to stop an incremental run.
- **Per-video caps**: We strictly cap retrieval at 500 comments per video per run to prevent viral videos from exhausting the quota.
- **Incomplete-stream statuses**: Videos that hit the cap before reaching the watermark are explicitly marked as `CAPPED_BEFORE_WATERMARK`.
- **Top-level-comment MVP**: Replies require separate 1-unit calls to `comments.list`, so they are deferred to Version 1.x.
- **Separate relevance audit sample**: We must make separate `order=relevance` API calls to evaluate algorithmic surfacing (RQ10), requiring isolated storage.

## Data Quality Risks
- **Edited comments**: Text can change post-ingestion. We rely on `updatedAt` for detection, but continuous polling is unfeasible.
- **Deleted comments**: Comments may vanish, breaking historical additivity if the warehouse attempts to mirror live state perfectly.
- **Unavailable author IDs**: Sometimes `authorChannelId` is null, requiring fallback pseudonymisation strategies.
- **Duplicate retrievals**: Pagination boundaries can shift during live ingestion, necessitating strict deduplication on `source_comment_id`.
- **Shifting page tokens**: A `nextPageToken` may become invalid if the underlying dataset shifts too drastically between runs.
- **Comments disabled**: Creators can disable comments, abruptly freezing the time-series.
- **Language detection errors**: The API does not consistently flag comment language, requiring internal NLP filtering.

## Privacy and Retention
- **Pseudonymisation**: All author identifiers (like `authorChannelId` or handles) must be one-way hashed using HMAC before entering the data warehouse.
- **Raw author ID handling**: Raw identifiers exist only transiently in memory during the extraction/loading phase.
- **HMAC key lifecycle**: The hashing key is rotated periodically or securely stored to prevent reverse-engineering of user identities.
- **Text retention**: Comment text is retained for NLP classification and narrative analysis but should not be indefinitely republished outside the platform context.
- **Deletion policy**: The project complies with data deletion requests and YouTube API terms regarding data retention.
- **Dashboard exposure restrictions**: The Streamlit dashboard exposes only aggregate metrics, never raw individual user comment streams or PII.

## Infrastructure Validation

A live API smoke test was completed successfully on 7 August 2026.

Observed result:
- API connection successful
- 10 top-level comments retrieved
- chronological ordering enabled
- pagination token returned
- comment timestamps available
- original comment text available

This confirms that the configured Google Cloud project and restricted API key can retrieve public YouTube comment threads.

The smoke test validates connectivity only. It does not validate sampling completeness, target resolution, language detection, or analytical suitability.

## Feasibility Decision
Based on the API capabilities and constraints, the project proceeds under the following determinations:

- Core MVP chronological ingestion: `SUPPORTED`
- Entity-targeted sentiment analysis: `SUPPORTED`
- Symmetric tone-vs-mix decomposition: `SUPPORTED`
- Returning author overlap metrics: `PARTIALLY_SUPPORTED` (bounded to observed sample only)
- Complete global audience migration: `NOT_SUPPORTED`
- Reply thread ingestion: `DEFERRED` (to v1.x)
- Matched-event comparison: `DEFERRED` (to v1.x)
- Relevance-surfacing audit: `DEFERRED` (to v1.x)
- Long-term narrative lifecycle: `DEFERRED` (to v1.x)
