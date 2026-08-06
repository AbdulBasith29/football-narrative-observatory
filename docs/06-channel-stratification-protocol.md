> ⚠️ This repository documents an active research and engineering project.
>
> The methodology is frozen for Version 1.1, while the implementation is under active development.
>
> **Methodology Revision Notice**: This protocol revises the channel taxonomy described in the earlier Version 1.0 Sampling Methodology and Analysis Design Document. It replaces a single mutually exclusive stratum list with a two-axis model (Channel Type and Player Focus) to safely distinguish publisher scope from observed audience attention.

# Channel Stratification Protocol

**Version**: 1.1
**Status**: Frozen for Version 1.1
**Last Updated**: August 2026

## Abstract
To accurately interpret shifts in aggregate sentiment and narrative adoption (e.g. symmetric tone-versus-mix decomposition), the Football Narrative Observatory must control for the baseline orientation of the sampled channels. 

This protocol answers the central question: *Using only evidence available before an event, how do we reproducibly assign a tracked YouTube channel to a documented audience-orientation stratum without using reactions from the event being analysed?*

## 1. The Unit of Classification
The fundamental unit of classification is a **single tracked YouTube channel at a specific point in time**. 
Because a channel's content strategy may evolve, a channel does not have a single eternal stratum. The warehouse maintains a Type-2 historically accurate mapping (`core.dim_channel_stratum_version`). 

Furthermore, to explicitly freeze the assignment used by each event analysis, every event window creates a pinned event-specific record (`core.bridge_event_channel_stratum_snapshot`).

## 2. The Two-Axis Classification Taxonomy
To prevent confounding publisher type with player bias, channels are classified along two independent dimensions. 
> [!IMPORTANT]
> Version 1.1 headline metric decompositions (e.g., M019) use the composite stratum of **Channel Type × Player Focus** (e.g., `BROAD_REACH_PUBLISHER × NO_STRONG_DOMINANT_PLAYER_FOCUS`), with rare combinations collapsed only according to a versioned minimum-sample rule.

### Axis A: Channel Type
- `BROAD_REACH_PUBLISHER`: Major sports networks, global broadcasters, and tier-1 football news aggregators.
- `CLUB_MEDIA`: Channels explicitly dedicated to covering a specific football club.
- `ANALYSIS_PUBLISHER`: Creators and outlets focused on tactical breakdowns, statistics, or general football punditry without club allegiance.
- `INDEPENDENT_CREATOR`: General individual creators, vloggers, and reaction channels.
- `OTHER`: Channels that do not fit the above categories.
- `UNCLASSIFIED`: Failed to meet minimum evidence requirements for Channel Type.

### Axis B: Player Focus
Focus measures *attention and coverage concentration*, not necessarily favourable sentiment.
- `MESSI_FOCUSED`: The channel's video catalog heavily and disproportionately mentions Lionel Messi.
- `RONALDO_FOCUSED`: The channel's video catalog heavily and disproportionately mentions Cristiano Ronaldo.
- `MIXED_FOCUS`: The channel frequently covers both players without heavily skewing toward one over the other.
- `NO_STRONG_DOMINANT_PLAYER_FOCUS`: No class crossed the strict dominance threshold.
- `UNCLASSIFIED`: Failed to meet minimum evidence requirements for Player Focus.

*(Note: `UNCLASSIFIED` status is explicitly axis-specific. A channel with a known Channel Type but unknown Player Focus remains eligible for type-only analysis).*

## 3. The Pre-Event Evidence Window
To prevent future leakage, classification relies strictly on the **Pre-Event Evidence Window**.

- **Exact Timestamps**:
  ```text
  classification_reference_period_end = event_start_at - exclusion_buffer
  ```
- **Example**: If an event begins at `2022-12-18T15:00:00Z` with a 24-hour `exclusion_buffer`, the reference window ends at `2022-12-17T15:00:00Z`.
- **Duration**: Protocol Version 1.1 uses a 90-day trailing window. Sensitivity checks compare 60-, 90-, and 180-day reference periods during protocol validation to ensure stability.

### Unscheduled Events
For unplanned events (e.g., unexpected transfers, sudden interviews), the evidence cutoff is the **earliest documented detection timestamp** minus the configured exclusion buffer. No evidence created after that point may be used.

## 4. Evidence Sources and Prohibited Evidence
### Allowed Evidence Sources
- The channel's "About" page description.
- Video titles published within the evidence window.
- Video descriptions and tags published within the evidence window.
- Versioned manual categorisations from recognised third-party databases.

### Entity Mentions and Multi-Label Counting
- **Direct Player Mentions**: Exact name aliases ("Messi", "Lionel Messi", "Cristiano", "CR7"). These strictly drive player-focus classification.
- **Context Entity Mentions**: Club/nation aliases. These only support classification when direct player relevance is also established within the same video metadata. All alias mappings are versioned.
- **Multi-Label Video Counting**: Player mention prevalence is multi-label at the video level. A single GOAT-debate video mentioning both Messi and Ronaldo adds $+1$ to both $V_M$ and $V_R$. Duplicated uploads and re-uploaded Shorts within the window are deduplicated prior to calculating prevalence.

### Prohibited Evidence
- **Any video published on or after the `classification_reference_period_end`.**
- Comment sentiment, audience overlap, or engagement metrics from the event window itself.

## 5. Minimum Evidence Requirements
To be eligible for programmatic classification, a channel must possess:
1. At least one reliable channel-identity signal (e.g., an English About description, or a third-party categorization).
2. A minimum of **10 distinct videos** published during the 90-day pre-event evidence window, **OR** a minimum of 10 videos in an extended 180-day fallback window.

Channels failing these requirements on a given axis receive an `UNCLASSIFIED` status for that axis. Fallback classifications (180-day) are explicitly excluded from headline decomposition (M019) and only included in sensitivity analyses or when sample coverage would otherwise completely fail.

## 6. Deterministic Classification Rules
### Channel Type Assignment
Channel type is resolved hierarchically using deterministic mathematical thresholds and versioned registries.

1. **Publisher Check**: If the channel ID matches a curated, version-controlled registry of global broadcasters (`BROAD_REACH_PUBLISHER` allowlist), assign `BROAD_REACH_PUBLISHER`.
2. **Club Check**: Let $P_{club,k}$ be the proportion of eligible videos explicitly associated with club $k$. 
   If $\max_k P_{club,k} > 0.70$ AND the winning club exceeds the second-ranked club by a configured margin, assign `CLUB_MEDIA`.
3. **Analysis Check**: If the channel ID matches a version-controlled registry of tactical creators, OR the analysis-topic tag share exceeds the configured threshold, assign `ANALYSIS_PUBLISHER`.
4. **Fallback**: Default to `INDEPENDENT_CREATOR` if identity signals are present but no specific type is triggered.

### Player Focus Assignment
Player focus evaluates video-level prevalence.

$$P_M = \frac{\text{eligible videos mentioning Messi}}{\text{all eligible videos}}$$

$$P_R = \frac{\text{eligible videos mentioning Ronaldo}}{\text{all eligible videos}}$$

$$R_{M:R} = \frac{V_M + \alpha}{V_R + \alpha}$$

*(Where $V$ is the raw volume of videos, and $\alpha = 1$ is a smoothing constant to prevent undefined ratios).*

1. If $R_{M:R} > 4.0$ and $P_M > 0.15$: Assign `MESSI_FOCUSED`.
2. If $R_{R:M} > 4.0$ and $P_R > 0.15$: Assign `RONALDO_FOCUSED`.
3. If both $P_M > 0.15$ and $P_R > 0.15$ AND $P_M + P_R \ge \text{combined\_focus\_threshold}$ (e.g., 0.40): Assign `MIXED_FOCUS`.
4. Otherwise: Assign `NO_STRONG_DOMINANT_PLAYER_FOCUS`.

## 7. Confidence and Stability Handling
Confidence is axis-specific.

### Player Focus Confidence (`player_focus_confidence`)
- **HIGH**: Large threshold margin, $>30$ eligible videos, and mathematically stable. (Temporal stability: The 90-day period is split into three 30-day subwindows; HIGH stability requires all three eligible subwindows to independently return the same class).
- **MEDIUM**: Clear threshold margin, but limited evidence ($10-30$ videos), or at least two of three subwindows agree.
- **LOW**: Within 10% of a decision boundary, conflicting subwindows, or relying on the 180-day fallback window.

### Channel Type Confidence (`channel_type_confidence`)
- **HIGH**: Explicit version-controlled registry match or overwhelming $P_{club,k}$ margin ($> 0.90$).
- **MEDIUM**: Supported by metadata agreement or third-party databases.
- **LOW**: Relying on fallback default (`INDEPENDENT_CREATOR`) with sparse metadata.

## 8. Event Pinning and Leakage Prevention
To explicitly freeze the assignment used by each event analysis, the pipeline inserts a pinned snapshot into `core.bridge_event_channel_stratum_snapshot` at grain `event_version × channel × protocol_version`.

The snapshot stores enough calculated evidence to reproduce the assignment:
- `channel_type_value` & `player_focus_value`
- `channel_type_confidence` & `player_focus_confidence`
- `reference_period_start` & `reference_period_end`
- `effective_window_days`
- `eligible_video_count`, `messi_video_count`, `ronaldo_video_count`
- `messi_prevalence`, `ronaldo_prevalence`, `focus_ratio`
- `fallback_used`
- `override_id`

## 9. Manual Review and Disagreement Resolution
Analysts may manually override a programmatic classification (e.g., for highly misleading clickbait). Manual overrides are strictly axis-specific.

**Governance Protocol**:
- Overrides must be submitted prior to the execution of the analytical event window pipeline.
- The analyst specifies: `classification_axis_overridden`, `original_axis_value`, `proposed_axis_value`, `classification_override_reason`, and `supporting_evidence`.
- Secondary reviewer approval is required.
- Overrides produce a discrete `review_status` (e.g., `APPROVED_OVERRIDE`); they **do not** silently elevate algorithmic confidence to `HIGH`.
- The override **does not mutate** the existing Type-2 row; it generates a new audited assignment.

## 10. Third-Party Categorisations
Recognized third-party databases may support `Channel Type` classification and manual review. Absence from a third-party database is not negative evidence. The protocol mandates tracking:
- `third_party_source_id`
- `source_version`
- `retrieved_at`
- `source_label`
- `mapped_internal_label`
- `mapping_rule_version`

## 11. Validation Framework
Human labels are the **adjudicated reference labels**. A stratified sample of channels must be independently labelled by at least two human reviewers.

To prevent threshold overfitting:
- **Development Set**: Used for threshold calibration (e.g., tuning the 4.1 ratio).
- **Validation Set**: Exclusively used for final protocol evaluation.
- Both sets are split at the channel level so videos from one channel cannot appear in both sets.

The validation report must document:
- Inter-rater agreement (Krippendorff's alpha).
- Per-class support, macro F1 score, and balanced accuracy against the human baseline.
- Separate validation results for Channel Type and Player Focus.
- `UNCLASSIFIED` rate across different window lengths.
- Override rates in production.

## 12. Periodic Reassessment
The ingestion pipeline executes batch reassessments prior to an event, or on a strict 90-day cadence.

Every reassessment creates an **append-only assessment record** (`ops.fact_channel_classification_assessment`) capturing the exact evidence and confidence at that moment. A new Type-2 `dim_channel_stratum_version` row is created **only** when either axis value formally changes. Confidence or minor evidence changes without a class shift remain preserved in the assessment history without churning the dimensional assignment.
