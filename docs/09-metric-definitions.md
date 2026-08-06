# Metric Definitions

**Version**: 1.1
**Status**: Frozen for Version 1.1

> **Methodology Revision Notice**: Protocol Version 1.1 replaces the legacy single-axis channel stratum model. All metric dimensions referencing `channel stratum` or `dim_channel_stratum_version` now explicitly refer to the Two-Axis Classification Taxonomy (Channel Type × Player Focus) composite stratum.

This document is the **statistical contract** governing every metric exposed by the platform. Every metric definition must conform strictly to these rules:
1. Every metric has one declared stable ID, version, and owner.
2. Every metric has one declared grain.
3. Every metric declares its eligible population.
4. Every metric defines numerator and denominator using exact mathematical notation.
5. Every metric defines null behaviour and exclusions.
6. Every metric defines permitted and prohibited interpretations.

### Display Status Definitions
- **Headline**: Visible at initial page load.
- **Core**: Standard page-level analytical metric.
- **Drill-down**: Exposed after user interaction.
- **Diagnostic**: Methodology or data-quality panel.
- **Exploratory**: Non-production or deferred analytical output.

### Uncertainty Policy
Inferential and comparative metrics must report an uncertainty estimate appropriate to their grain and dependency structure (e.g., cluster bootstrap by video, or paired bootstrap for shared-author metrics). Comment-level independence must not be assumed where observations are clustered within videos, channels, authors, or events. 

Descriptive pipeline and coverage metrics report exact observed values, sample counts, and completeness status. Confidence intervals are not added where they would imply unsupported population inference.

---

## 1. Methodological & Diagnostic Metrics
*Used to monitor pipeline health, data loss, and coverage.*

### M001_CHRONOLOGICAL_CORPUS_RETENTION
- **Metric Name**: Chronological Corpus Retention Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Diagnostic
- **Purpose**: Measure the proportion of the retrieved corpus that survives filtering into the chronological sample.
- **Research Question(s)**: N/A
- **Grain**: One event window
- **Dependencies**: `fact_comment`, `dim_window`, `sampling_policy_version`
- **Formula**: $N_{chronological} / N_{retrieved\_canonical}$
- **Numerator**: Count of comments in the primary chronological corpus.
- **Denominator**: Distinct canonical comments observed through chronological retrieval for the selected event window before analytical inclusion rules are applied.
- **Population**: Retrieved canonical corpus.
- **Exclusions**: Relevance-audit records and non-canonical duplicates.
- **Null Behaviour**: NULL if $N_{retrieved\_canonical} = 0$.
- **Units**: Percentage
- **Interpretation**: The retention rate after applying deduplication, spam, and chronological capping rules.
- **Forbidden Interpretation**: A measure of API stability or global fan volume.

### M030_ANALYTICAL_SAMPLE_RETENTION
- **Metric Name**: Analytical Sample Retention Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Diagnostic
- **Purpose**: Measure the proportion of the chronological corpus that survives target-opinion filtering into the final analytical sample.
- **Research Question(s)**: N/A
- **Grain**: One event window
- **Dependencies**: `fact_comment_target_opinion`, `fact_comment`
- **Formula**: $N_{analytical} / N_{chronological}$
- **Numerator**: Distinct canonical comments explicitly included in the target-opinion analytical subset.
- **Denominator**: Count of comments in the primary chronological corpus.
- **Population**: Primary chronological corpus.
- **Exclusions**: None.
- **Null Behaviour**: NULL if $N_{chronological} = 0$.
- **Units**: Percentage
- **Interpretation**: The analytical retention rate mapping discourse to valid targets.
- **Forbidden Interpretation**: A measure of overall NLP model accuracy.

### M002_TARGET_RESOLUTION_RATE
- **Metric Name**: Target Resolution Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Diagnostic
- **Purpose**: Measure the success rate of entity resolution within English comments.
- **Research Question(s)**: N/A
- **Grain**: One event window
- **Dependencies**: `fact_comment`, `target_resolution_model_version`
- **Formula**: $N_{resolved} / N_{english}$
- **Numerator**: Count of English comments with a successfully resolved target.
- **Denominator**: Count of English comments in the primary chronological corpus.
- **Population**: English-language comments in the primary chronological corpus.
- **Exclusions**: `LOW_CONFIDENCE`, `AMBIGUOUS`, and `MODEL_ERROR` are treated as unresolved (denominator only).
- **Null Behaviour**: NULL if $N_{english} = 0$.
- **Units**: Percentage
- **Interpretation**: The NLP pipeline's ability to confidently map discourse to a known entity.
- **Forbidden Interpretation**: The percentage of comments actually discussing a football player.

### M003_LANGUAGE_PASS_RATE
- **Metric Name**: Language Pass Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Diagnostic
- **Purpose**: Measure the proportion of comments identified as English.
- **Research Question(s)**: N/A
- **Grain**: One event window
- **Dependencies**: `fact_comment`, `language_model_version`
- **Formula**: $N_{english} / N_{chronological}$
- **Numerator**: Count of comments classified as English above the confidence threshold.
- **Denominator**: Count of comments in the primary chronological corpus.
- **Population**: Primary chronological corpus.
- **Exclusions**: None.
- **Null Behaviour**: NULL if $N_{chronological} = 0$.
- **Units**: Percentage
- **Interpretation**: The proportion of the sample eligible for target-opinion analysis.
- **Forbidden Interpretation**: The true linguistic composition of a channel's audience.

### M004_AUTHOR_ID_COVERAGE
- **Metric Name**: Author-ID Coverage Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Diagnostic
- **Purpose**: Measure the proportion of comments that can be traced to a stable pseudonymised author.
- **Research Question(s)**: RQ3
- **Grain**: One event window
- **Dependencies**: `fact_comment`
- **Formula**: $N_{stable\_author} / N_{chronological}$
- **Numerator**: Count of comments in the primary chronological corpus with a stable author hash.
- **Denominator**: Count of comments in the primary chronological corpus.
- **Population**: Primary chronological corpus.
- **Exclusions**: None.
- **Null Behaviour**: NULL if $N_{chronological} = 0$.
- **Units**: Percentage
- **Interpretation**: Essential context for interpreting participation overlap and author-weighted metrics.
- **Forbidden Interpretation**: The percentage of authentic human users (vs bots).

### M005_COMPLETE_WINDOW_COVERAGE
- **Metric Name**: Complete-Window Coverage Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Diagnostic
- **Purpose**: Measure the proportion of intended videos that achieved an accepted continuity status for the analysed window.
- **Research Question(s)**: N/A
- **Grain**: One event window $\times$ channel stratum
- **Dependencies**: `dim_video`, `ingestion_run_log`
- **Formula**: $V_{complete\_status} / V_{intended}$
- **Numerator**: Count of intended eligible videos where `continuity_status IN ('COMPLETE_TO_WATERMARK', 'COMPLETE_AFTER_BACKFILL')`.
- **Denominator**: Count of intended eligible videos in the sampling configuration.
- **Population**: Selected video sample.
- **Exclusions**: `POTENTIAL_GAP`, `GAP_UNRESOLVED`, and capped runs are excluded from the numerator.
- **Null Behaviour**: NULL if $V_{intended} = 0$.
- **Units**: Percentage
- **Interpretation**: Shows if API quota exhaustion compromised the intended sample.
- **Forbidden Interpretation**: Proportion of YouTube coverage analyzed.

### M006_NARRATIVE_CLASSIFICATION_COVERAGE
- **Metric Name**: Narrative Classification Coverage Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Diagnostic
- **Purpose**: Measure the proportion of target opinions successfully mapped to a defined narrative instance.
- **Research Question(s)**: RQ5
- **Grain**: One event window
- **Dependencies**: `fact_comment_target_opinion`, `bridge_target_opinion_narrative`, `narrative_model_version`
- **Formula**: $N_{classified} / N_{target\_opinions}$
- **Numerator**: Target opinions with at least one accepted narrative classification.
- **Denominator**: All eligible target opinions.
- **Population**: Target-opinion analytical subset.
- **Exclusions**: None.
- **Null Behaviour**: NULL if $N_{target\_opinions} = 0$.
- **Units**: Percentage
- **Interpretation**: The proportion of eligible target opinions for which the current taxonomy and model produced at least one accepted narrative assignment.
- **Forbidden Interpretation**: The proportion of discourse that objectively contains a coherent narrative.

### M007_CLASSIFIER_OUTPUT_COVERAGE
- **Metric Name**: Classifier Output Coverage Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Diagnostic
- **Purpose**: Track the rate at which NLP models abstain from scoring (e.g. for derision or toxicity).
- **Research Question(s)**: N/A
- **Grain**: One event window $\times$ specific classifier
- **Dependencies**: `fact_comment_target_opinion`, `model_version`
- **Formula**: $N_{accepted\_outputs} / N_{target\_opinions}$
- **Numerator**: Count of target opinions where the classifier produced an accepted output (excluding `LOW_CONFIDENCE`, `AMBIGUOUS`, `MODEL_ERROR`).
- **Denominator**: All eligible target opinions.
- **Population**: Target-opinion analytical subset.
- **Null Behaviour**: NULL if $N_{target\_opinions} = 0$.
- **Units**: Percentage
- **Interpretation**: The model's ability to confidently score the discourse.
- **Forbidden Interpretation**: Proof that unclassified comments objectively lack the feature (e.g., contain no derision).

---

## 2. Foundational Target-Opinion Metrics
*Core metrics quantifying tone, toxicity, and stance.*

### M008_COMMENT_WEIGHTED_MEAN_TARGET_VALENCE
- **Metric Name**: Comment-Weighted Mean Target Valence
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Headline
- **Purpose**: Measure the average expressed sentiment towards a player across all comments.
- **Research Question(s)**: RQ1
- **Grain**: One event $\times$ one target player $\times$ one window
- **Dependencies**: `fact_comment_target_opinion`
- **Formula**: $\frac{1}{N} \sum s_i$
- **Numerator**: Sum of valence scores for all eligible target opinions.
- **Denominator**: Count of eligible target opinions ($N$).
- **Population**: Target-opinion analytical subset.
- **Exclusions**: Unresolved targets.
- **Null Behaviour**: NULL if $N = 0$.
- **Units**: Valence Score ($-1.0$ to $+1.0$)
- **Interpretation**: The aggregate tone expressed in the observed sample. Each eligible target opinion receives equal weight. The metric is therefore sensitive to comment-volume concentration and must be shown with channel/video composition context.
- **Forbidden Interpretation**: Absolute public approval rating.

### M009_VIDEO_BALANCED_MEAN_TARGET_VALENCE
- **Metric Name**: Video-Balanced Mean Target Valence
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Diagnostic
- **Purpose**: Measure the average expressed sentiment towards a player without allowing viral videos to dominate.
- **Research Question(s)**: RQ1
- **Grain**: One event $\times$ one target player $\times$ one window
- **Dependencies**: `fact_comment_target_opinion`, `dim_video`
- **Formula**: $\frac{1}{V} \sum_{v=1}^{V} \bar{S}_v$
- **Numerator**: Sum of the mean valence scores per video ($\bar{S}_v$).
- **Denominator**: Count of eligible videos containing target opinions ($V$).
- **Population**: Target-opinion analytical subset grouped by video.
- **Null Behaviour**: NULL if $V = 0$.
- **Units**: Valence Score ($-1.0$ to $+1.0$)
- **Interpretation**: The video-averaged sentiment, providing a sensitivity check against comment-weighted metrics.
- **Forbidden Interpretation**: The true average sentiment of individual fans.

### M010_VALENCE_CATEGORY_SHARE
- **Metric Name**: Positive / Neutral / Negative Share
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Core
- **Purpose**: Show the categorical distribution of target-specific valence labels.
- **Research Question(s)**: RQ1
- **Grain**: One event $\times$ one target player $\times$ one window
- **Dependencies**: `fact_comment_target_opinion`
- **Formula**: $N_{category} / N_{target\_opinions}$
- **Numerator**: Count of target opinions classified into the specific categorical band (e.g. Positive).
- **Denominator**: Count of all target opinions ($N$).
- **Population**: Target-opinion analytical subset.
- **Exclusions**: Unresolved targets.
- **Null Behaviour**: NULL if $N = 0$.
- **Units**: Percentage (The three shares must sum to $100\%$ within numerical tolerance).
- **Interpretation**: The categorical distribution of target-specific valence labels within the eligible sample.
- **Forbidden Interpretation**: Categorical split of sentiment "intensity", or true global fan sentiment distribution.

### M011_TARGET_SPECIFIC_DERISION_RATE
- **Metric Name**: Target-Specific Derision Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Core
- **Purpose**: Measure football-specific banter and mocking targeted at a player.
- **Research Question(s)**: RQ1, RQ4
- **Grain**: One event $\times$ one target player $\times$ one window
- **Dependencies**: `fact_comment_target_opinion`, `derision_model_version`
- **Formula**: $N_{derisive} / N_{accepted\_derision\_outputs}$
- **Numerator**: Count of target opinions classified as derisive.
- **Denominator**: Count of target opinions with accepted derision outputs.
- **Population**: Target-opinion analytical subset.
- **Exclusions**: Unresolved targets and model abstentions.
- **Null Behaviour**: NULL if $N_{accepted\_derision\_outputs} = 0$.
- **Units**: Percentage
- **Interpretation**: The concentration of derision within discourse about a specific player.
- **Forbidden Interpretation**: Proportion of toxic users on the platform.

### M012_TARGET_SPECIFIC_TOXICITY_RATE
- **Metric Name**: Target-Specific Toxicity Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Core
- **Purpose**: Measure overtly abusive, hateful, or harmful language targeted at a player.
- **Research Question(s)**: RQ4
- **Grain**: One event $\times$ one target player $\times$ one window
- **Dependencies**: `fact_comment_target_opinion`, `toxicity_model_version`
- **Formula**: $N_{toxic} / N_{accepted\_toxicity\_outputs}$
- **Numerator**: Count of target opinions classified as toxic.
- **Denominator**: Count of target opinions with accepted toxicity outputs.
- **Population**: Target-opinion analytical subset.
- **Exclusions**: Unresolved targets and model abstentions.
- **Null Behaviour**: NULL if $N_{accepted\_toxicity\_outputs} = 0$.
- **Units**: Percentage
- **Interpretation**: The concentration of severe abuse within discourse about a specific player.
- **Forbidden Interpretation**: Evidence of systemic platform toxicity.

### M013_SARCASM_RATE
- **Metric Name**: Sarcasm Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Drill-down
- **Purpose**: Measure the prevalence of sarcastic delivery in target opinions.
- **Research Question(s)**: RQ7
- **Grain**: One event $\times$ one target player $\times$ one window
- **Dependencies**: `fact_comment_target_opinion`, `sarcasm_model_version`
- **Formula**: $N_{sarcastic} / N_{accepted\_sarcasm\_outputs}$
- **Numerator**: Count of target opinions classified as sarcastic.
- **Denominator**: Count of target opinions with accepted sarcasm outputs.
- **Population**: Target-opinion analytical subset.
- **Exclusions**: Unresolved targets and model abstentions.
- **Null Behaviour**: NULL if $N_{accepted\_sarcasm\_outputs} = 0$.
- **Units**: Percentage
- **Interpretation**: Identifies highly contextual discourse formats.
- **Forbidden Interpretation**: Absolute measure of community wit.

### M014_DISCOURSE_MODEL_DISAGREEMENT_RATE
- **Metric Name**: Discourse Model Disagreement Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Diagnostic
- **Purpose**: Measure the gap between generic NLP models and domain-specific models for the same construct.
- **Research Question(s)**: RQ7
- **Grain**: One construct comparison (e.g., generic toxicity vs domain toxicity) $\times$ one window
- **Dependencies**: `fact_comment_target_opinion`
- **Required Configuration**: `construct_name`, `label_mapping_version`, `generic_model_version`, `domain_model_version`
- **Formula**: $N_{\hat{y}_{generic} \neq \hat{y}_{domain}} / N_{both\_accepted}$
- **Numerator**: Comments where the two model specifications produce different accepted labels, using the declared label-mapping rule.
- **Denominator**: Comments successfully scored by both models with accepted outputs.
- **Population**: Target-opinion analytical subset.
- **Null Behaviour**: NULL if $N_{both\_accepted} = 0$.
- **Units**: Percentage
- **Interpretation**: Frequency with which the two model specifications produce different accepted labels.
- **Forbidden Interpretation**: Proof that one model is objectively "wrong" or "better". Difference alone does not prove improvement.

### M015_TARGET_VALENCE_POLARISATION_INDEX
- **Metric Name**: Target-Valence Polarisation Index
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.x | **Display Status**: Exploratory
- **Purpose**: Measure the degree of sentiment polarisation (bimodal distribution) towards a player.
- **Research Question(s)**: RQ5
- **Grain**: One event $\times$ one target player $\times$ one window
- **Formula**: TBD (Requires structural validation beyond simple positive/negative variance).

---

## 3. Participation Metrics
*Used to observe author presence and volume dynamics.*

### M016_OBSERVED_AUTHOR_OVERLAP
- **Metric Name**: Observed Author Overlap
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Headline
- **Purpose**: Measure the stability of the observed author pool across two temporal windows.
- **Research Question(s)**: RQ3
- **Grain**: One event $\times$ one target player $\times$ two comparison windows
- **Dependencies**: `fact_comment_target_opinion`, `dim_window`, `author_hash`
- **Formula**: $\frac{|A_{pre} \cap A_{post}|}{|A_{pre} \cup A_{post}|}$
- **Numerator**: Count of distinct observed authors present in *both* windows for that player ($|A_{pre} \cap A_{post}|$).
- **Denominator**: Count of distinct observed authors present in *either* window for that player ($|A_{pre} \cup A_{post}|$).
- **Population**: Observed authors with accepted target-specific participation.
- **Target-Assignment Rule**: One logical comment contributes at most once per author $\times$ target player $\times$ window for participation membership.
- **Exclusions**: Authors without a stable ID hash.
- **Null Behaviour**: NULL if both windows contain zero observed authors.
- **Units**: Proportion (Jaccard Index)
- **Interpretation**: The proportion of distinct observed authors appearing in either window who were observed in both.
- **Forbidden Interpretation**: Percentage of global fans, true user retention, platform migration, or lifetime activity.

### M017_NEWLY_OBSERVED_AUTHOR_CONTRIBUTION
- **Metric Name**: Newly Observed Author Contribution
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Core
- **Purpose**: Measure the volume of target-specific reaction attributable to authors who were silent in the baseline period.
- **Research Question(s)**: RQ3
- **Grain**: One event $\times$ one target player $\times$ post-event window
- **Dependencies**: `fact_comment_target_opinion`, `dim_window`, `author_hash`
- **Formula**: $C_{new} / C_{post\_identifiable}$
- **Numerator**: Count of post-event target-opinion comments ($C_{new}$) by newly observed identifiable authors for that player.
- **Denominator**: Count of all post-event target-opinion comments for that player with stable author IDs ($C_{post\_identifiable}$).
- **Population**: Target-opinion analytical subset.
- **Target-Assignment Rule**: One logical comment contributes at most once per author $\times$ target player $\times$ window.
- **Exclusions**: Comments without a stable ID hash are strictly excluded from both numerator and denominator.
- **Null Behaviour**: NULL if $C_{post\_identifiable} = 0$.
- **Units**: Percentage
- **Interpretation**: The proportion of the reaction attributable within the observed sample to comments from newly observed authors.
- **Forbidden Interpretation**: The influx of brand new YouTube accounts or new football fans.

---

## 4. Decomposition & Tone Shift Metrics
*Used to isolate structural volume changes from true sentiment changes.*

### M018_AUTHOR_WEIGHTED_RETURNING_AUTHOR_TONE_SHIFT
- **Metric Name**: Author-Weighted Returning-Author Tone Shift
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Headline
- **Purpose**: Measure the change in expressed tone among the stable returning audience, weighted evenly by author to prevent high-volume commenters from dominating the metric.
- **Research Question(s)**: RQ1, RQ3
- **Grain**: One event $\times$ one target player
- **Dependencies**: `fact_comment_target_opinion`, `dim_window`, `author_hash`
- **Formula**: $\frac{1}{|A_{shared}|} \sum_{a} \left( \bar{S}_{a,post} - \bar{S}_{a,pre} \right)$
- **Numerator**: The sum of each shared author's average valence shift ($\Delta S_a = \bar{S}_{a,post} - \bar{S}_{a,pre}$).
- **Denominator**: The count of shared authors ($|A_{shared}|$).
- **Population**: Target-opinion analytical subset, restricted to authors with accepted target-specific participation in *both* windows ($A_{pre} \cap A_{post}$).
- **Eligibility**: A shared author must have at least one accepted target opinion in each comparison window.
- **Uncertainty Method**: Paired bootstrap over shared authors, preserving all observations belonging to the sampled author within each replicate.
- **Null Behaviour**: NULL if $|A_{shared}| = 0$.
- **Units**: Valence Shift ($-2.0$ to $+2.0$)
- **Interpretation**: Shows a change in the observed expressed tone of returning authors.
- **Forbidden Interpretation**: Proof that the entire community "changed their minds."

### M019_CHANNEL_COMPOSITION_EFFECT
- **Metric Name**: Channel Composition Effect
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Headline
- **Purpose**: Measure the portion of aggregate sentiment change associated with changes in the observed channel-stratum composition.
- **Research Question(s)**: RQ2
- **Grain**: One event $\times$ one target player
- **Dependencies**: `fact_comment_target_opinion`, `dim_channel_stratum_version`
- **Formula**: $\sum_c \left[ \frac{\bar{S}_{c,0} + \bar{S}_{c,1}}{2} \times (w_{c,1}^* - w_{c,0}^*) \right]$
- **Population**: Target-opinion analytical subset spanning the pre-event ($0$) and post-event ($1$) windows.
- **Eligibility**: Stratum must meet the configured minimum target-opinion count in *both* windows to be considered a Shared Stratum.
- **Weighting Universe**: Weights are renormalised across eligible shared strata separately in each window ($w_{c,t}^* = \frac{n_{c,t}}{\sum_{j \in Shared} n_{j,t}}$).
- **Exclusions**: Unpaired strata are excluded from the additive decomposition identity and reported descriptively.
- **Null Behaviour**: NULL if shared comparative strata are insufficient.
- **Units**: Valence Contribution ($-2.0$ to $+2.0$)
- **Interpretation**: The sentiment shift contribution associated with shifting target-opinion volume shares between eligible channel strata. Baseline and post-event shared-strata coverage rates must be disclosed alongside this metric.
- **Forbidden Interpretation**: A causal effect, or proof that individual users migrated between channels.

### M020_WITHIN_STRATUM_TONE_EFFECT
- **Metric Name**: Within-Stratum Tone Effect
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Headline
- **Purpose**: Measure the portion of aggregate sentiment change associated with changes in mean target-specific tone within shared channel strata.
- **Research Question(s)**: RQ2
- **Grain**: One event $\times$ one target player
- **Dependencies**: `fact_comment_target_opinion`, `dim_channel_stratum_version`
- **Formula**: $\sum_c \left[ \frac{w_{c,0}^* + w_{c,1}^*}{2} \times (\bar{S}_{c,1} - \bar{S}_{c,0}) \right]$
- **Population**: Target-opinion analytical subset.
- **Eligibility**: Same as Channel Composition Effect.
- **Exclusions**: Unpaired strata.
- **Null Behaviour**: NULL if shared comparative strata are insufficient.
- **Units**: Valence Contribution ($-2.0$ to $+2.0$)
- **Interpretation**: The contribution associated with changes in mean target-specific tone within shared channel strata.
- **Forbidden Interpretation**: Causal media influence.

### M021_STRATUM_ADJUSTED_SENTIMENT
- **Metric Name**: Stratum-Adjusted Sentiment
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Core
- **Purpose**: Control for persistent stratum orientation to isolate abnormal reactions.
- **Research Question(s)**: RQ1
- **Grain**: One event $\times$ one target player $\times$ one channel stratum
- **Dependencies**: `fact_comment_target_opinion`, `dim_channel_stratum_version`
- **Formula**: $\bar{S}_{event} - \bar{S}_{baseline}$
- **Population**: Target-opinion analytical subset.
- **Eligibility**: Requires the configured minimum target-opinion count and complete baseline coverage.
- **Leakage Rule**: No event-period or later observations may enter the baseline. The baseline is exclusively the versioned pre-event reference period defined in the event catalogue.
- **Exclusions**: Strata failing minimum observation thresholds.
- **Null Behaviour**: NULL if the baseline lacks sufficient observations.
- **Units**: Valence Shift ($-2.0$ to $+2.0$)
- **Interpretation**: Deviation from the versioned pre-event mean for the same player and channel stratum.
- **Forbidden Interpretation**: The channel's "normal audience response", causal media influence, or absolute sentiment.

---

## 5. Narrative & Stance Metrics
*Used to quantify the presence of specific discourse themes and claims.*

### M022_PRIMARY_NARRATIVE_SHARE
- **Metric Name**: Primary Narrative Share
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Headline
- **Purpose**: Track the dominant footprint of a specific narrative claim within overall discussion.
- **Research Question(s)**: RQ5
- **Grain**: One event $\times$ one target player $\times$ one primary narrative instance
- **Dependencies**: `fact_comment_target_opinion`, `bridge_target_opinion_narrative`, `dim_narrative_instance`
- **Formula**: $N_{primary\_instance} / N_{accepted\_primary}$
- **Numerator**: Target opinions where the instance is assigned as the primary narrative via the bridge table.
- **Denominator**: All target opinions with an accepted primary narrative.
- **Population**: Target-opinion analytical subset.
- **Exclusions**: Target opinions without a primary narrative.
- **Null Behaviour**: NULL if $N_{accepted\_primary} = 0$.
- **Units**: Percentage (Shares sum to $100\%$)
- **Interpretation**: The core narrative distribution.
- **Forbidden Interpretation**: The proportion of users who believe the narrative is objectively true.

### M023_MULTI_LABEL_PREVALENCE
- **Metric Name**: Multi-Label Prevalence
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Core
- **Purpose**: Track the total footprint of a specific narrative, including secondary instances.
- **Research Question(s)**: RQ5
- **Grain**: One event $\times$ one target player $\times$ one narrative instance
- **Dependencies**: `fact_comment_target_opinion`, `bridge_target_opinion_narrative`, `dim_narrative_instance`
- **Formula**: $N_{instance} / N_{eligible\_opinions}$
- **Numerator**: Target opinions assigned to the instance via the bridge table (primary or secondary).
- **Denominator**: All eligible target opinions.
- **Population**: Target-opinion analytical subset.
- **Null Behaviour**: NULL if $N_{eligible\_opinions} = 0$.
- **Units**: Percentage (Shares across narratives may exceed $100\%$)
- **Interpretation**: The total visibility of a narrative claim.
- **Forbidden Interpretation**: Mutually exclusive share of voice.

### M024_BROAD_REACH_NARRATIVE_SHARE
- **Metric Name**: Broad-Reach Narrative Share
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Core
- **Purpose**: Measure the share of accepted narrative assignments observed within the `BROAD_REACH_PUBLISHER` stratum.
- **Research Question(s)**: RQ5, RQ7
- **Grain**: One event $\times$ one target player $\times$ one narrative instance
- **Dependencies**: `fact_comment_target_opinion`, `bridge_target_opinion_narrative`, `dim_narrative_instance`, `dim_channel_stratum_version`
- **Formula**: $N_{broad\_reach} / N_{all\_strata}$
- **Numerator**: Narrative-instance assignments occurring exclusively within the `BROAD_REACH_PUBLISHER` stratum.
- **Denominator**: All narrative-instance assignments across all channel strata.
- **Population**: Target-opinion analytical subset for a specified event.
- **Exclusions**: The dashboard should show the `UNCLASSIFIED` share separately, as a large unclassified population affects interpretation. `UNCLASSIFIED` is strictly excluded from the broad-reach numerator.
- **Null Behaviour**: NULL if $N_{all\_strata} = 0$.
- **Units**: Percentage
- **Interpretation**: A cross-sectional distribution of narrative assignments across documented channel strata.
- **Forbidden Interpretation**: "Diffusion" over time, or proof that mainstream media adopted or endorsed the narrative.

### M025_NARRATIVE_SUPPORT_RATE
- **Metric Name**: Narrative Support Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Core
- **Purpose**: Measure the proportion of narrative mentions that explicitly support the claim.
- **Research Question(s)**: RQ3, RQ4, RQ5
- **Grain**: One event $\times$ one target player $\times$ one narrative instance
- **Dependencies**: `bridge_target_opinion_narrative`, `stance_model_version`
- **Formula**: $N_{supports} / N_{accepted\_stance\_assignments}$
- **Numerator**: Target opinions supporting the narrative instance.
- **Denominator**: Target opinions with accepted stance assignments for that narrative.
- **Population**: Narrative assignments.
- **Exclusions**: Model abstentions.
- **Null Behaviour**: NULL if $N_{accepted\_stance\_assignments} = 0$.
- **Units**: Percentage
- **Interpretation**: Identifies explicit community support for a narrative claim.
- **Forbidden Interpretation**: The proportion of all users who support the narrative.

### M026_NARRATIVE_OPPOSITION_RATE
- **Metric Name**: Narrative Opposition Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Core
- **Purpose**: Measure the proportion of narrative mentions that explicitly oppose the claim.
- **Research Question(s)**: RQ3, RQ4, RQ5
- **Grain**: One event $\times$ one target player $\times$ one narrative instance
- **Dependencies**: `bridge_target_opinion_narrative`, `stance_model_version`
- **Formula**: $N_{opposes} / N_{accepted\_stance\_assignments}$
- **Numerator**: Target opinions opposing the narrative instance.
- **Denominator**: Target opinions with accepted stance assignments for that narrative.
- **Population**: Narrative assignments.
- **Exclusions**: Model abstentions.
- **Null Behaviour**: NULL if $N_{accepted\_stance\_assignments} = 0$.
- **Units**: Percentage
- **Interpretation**: Identifies explicit community pushback against a narrative claim.
- **Forbidden Interpretation**: The proportion of all users who oppose the narrative.

### M027_STANCE_CLASSIFICATION_COVERAGE
- **Metric Name**: Stance Classification Coverage
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.0 | **Display Status**: Diagnostic
- **Purpose**: Measure the rate at which stance models produce accepted outputs.
- **Research Question(s)**: N/A
- **Grain**: One event $\times$ one target player $\times$ one narrative instance
- **Dependencies**: `bridge_target_opinion_narrative`, `stance_model_version`
- **Formula**: $N_{accepted\_stance\_assignments} / N_{eligible\_narrative\_assignments}$
- **Numerator**: Narrative assignments with accepted stance outputs.
- **Denominator**: All eligible narrative assignments.
- **Units**: Percentage
- **Interpretation**: The model's ability to confidently score stance orientation.
- **Forbidden Interpretation**: Proof that unclassified assignments contain no explicit stance.

---

## 6. Comparative Metrics (Deferred)
*Advanced metrics requiring longitudinal modelling or historical event histories.*

### M028_STANDARDISED_REACTION
- **Metric Name**: Standardised Reaction
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.x | **Display Status**: Exploratory
- **Purpose**: Compare how unusual a response was across different players or events.
- **Research Question(s)**: RQ9
- **Grain**: One event $\times$ one target player
- **Dependencies**: `fact_comment_target_opinion`
- **Formula**: $Z = \frac{\bar{S}_{event} - \mu_{historical\_windows}}{\sigma_{historical\_windows}}$
- **Numerator**: Difference between event sentiment and historical mean sentiment across prior windows.
- **Denominator**: The standard deviation of sentiment measured across historical window-level or video-level aggregates (not individual comments).
- **Population**: Target-opinion analytical subset.
- **Null Behaviour**: NULL if $\sigma_{historical\_windows} = 0$.
- **Units**: Z-Score (Standard Deviations)
- **Interpretation**: Compares the event reaction with variation across comparable prior windows.
- **Forbidden Interpretation**: A definitive ranking of emotional intensity across unrelated time periods.

### M029_CROSS_STRATUM_PARTICIPATION_RATE
- **Metric Name**: Cross-Stratum Participation Rate
- **Metric Definition Version**: 1.0 | **Owner**: Analytics Methodology | **Last Reviewed**: 2026-08-07
- **Release Status**: V1.x | **Display Status**: Exploratory
- **Purpose**: Measure the proportion of newly observed authors in a target stratum who had prior observed activity in another specified stratum.
- **Research Question(s)**: RQ3
- **Grain**: One event $\times$ target channel stratum
- **Dependencies**: `fact_comment_target_opinion`, `dim_channel_stratum_version`, `author_hash`
- **Formula**: $A_{cross\_participating} / A_{newly\_observed}$
- **Numerator**: Count of newly observed commenters in the target stratum who have prior documented activity in a partisan origin stratum.
- **Denominator**: Count of all newly observed commenters in the target stratum.
- **Required Configuration**: `lookback_window`, `origin_stratum_set`, `target_stratum`, `target_player_consistency_rule`, `multi-origin_author_counting_rule`, `minimum_author_id_coverage`.
- **Population**: Target-opinion analytical subset.
- **Exclusions**: Authors without stable ID hashes.
- **Null Behaviour**: NULL if $A_{newly\_observed} = 0$.
- **Units**: Percentage
- **Interpretation**: Observed cross-stratum participation among identifiable authors.
- **Forbidden Interpretation**: Actual audience migration, flow, lifetime viewing behaviour, or a permanent shift in loyalty.

---

## Metric Invariants

The data warehouse and analytical layers must validate the following invariants:
- **M010** category shares must sum to 100% within tolerance.
- **M019** + **M020** = shared-universe sentiment shift ($ΔS_{shared}$) within tolerance.
- **M022** primary narrative shares must sum to 100% within an eligible cohort.
- All percentage metrics must lie between $0$ and $100\%$.
- All valence metrics must lie within their declared bounds ($-1.0$ to $+1.0$, or $-2.0$ to $+2.0$ for shifts/contributions).
- No metric is shown when its minimum cohort or coverage threshold fails.
