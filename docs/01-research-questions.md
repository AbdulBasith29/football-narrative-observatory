# Research Questions

This document defines the research questions governing the Football Narrative Observatory. These questions determine the sampling design, source requirements, warehouse grains, NLP outputs, analytical marts, and Streamlit investigations.

All questions apply only to the project’s documented sample of selected English-language YouTube videos and comments. Unless explicitly stated otherwise, findings describe observed associations and accounting decompositions rather than causal effects.

## Primary Research Questions

### RQ1. Overall reaction shifts
**How do observed target-specific discourse patterns toward Messi and Ronaldo change before, during, and after selected football events?**
*This is the broadest question and anchors the entire platform.*

**Supports**:
- **Required for Version 1.0**: Targeted player reaction mart (`mart_targeted_player_reaction`, provisional)
- **Planned analytical output**: Streamlit Event Overview Dashboard

### RQ2. Tone versus channel composition
**When aggregate target-specific sentiment changes, how much of that shift is associated with changes in observed tone within shared channel strata, and how much is associated with changes in the composition of sampled channel strata?**
*This question is addressed using the symmetric tone-versus-mix decomposition. It describes aggregate observed shifts and does not establish individual opinion change.*

**Supports**:
- **Required for Version 1.0**: Channel shift decomposition mart (`mart_channel_shift_decomposition`, provisional)
- **Required for Version 1.0**: Versioned channel-stratum dimension (`dim_channel_stratum_version`, provisional)
- **Planned analytical output**: Streamlit Decomposition Explorer

### RQ3. Returning versus newly observed authors
**Among authors observed in both the pre-event and post-event samples, how does observed target-specific stance differ between the two windows, and how much of the aggregate post-event reaction is associated with authors newly observed in the sampled discussion?**
*This question distinguishes within-sample returning-author behaviour from changes in observed author composition without claiming complete lifetime author histories.*

**Supports**:
- **Required for Version 1.0**: Observed author-window fact (`fact_observed_author_window`, provisional)
- **Required for Version 1.0**: Observed author overlap mart (`mart_observed_author_overlap`, provisional)
- **Planned analytical output**: Streamlit Audience Dynamics Module

## Secondary Research Questions

### RQ4. Target-specific treatment
**In comments that mention multiple players, how are Messi and Ronaldo evaluated differently within the same piece of discourse?**
*This justifies entity-targeted sentiment rather than one global sentiment label per comment.*

**Supports**:
- **Required for Version 1.0**: Comment target-opinion fact (`fact_comment_target_opinion`, provisional)
- **Required for Version 1.0**: Comment-target bridge (`bridge_comment_target`, provisional)
- **Required for Version 1.0**: NLP Entity Resolution Model
- **Planned analytical output**: Streamlit Target Treatment Explorer

### RQ5. Narrative drivers
**Which context-specific narrative instances are most frequently associated with positive or negative target-specific valence, elevated derision, or highly polarised reaction distributions toward each player?**
*Examples could include: award legitimacy, league quality, refereeing favouritism, international legacy, age and decline, GOAT comparisons.*

**Supports**:
- **Required for Version 1.0**: Daily narrative instance mart (`mart_narrative_instance_daily`, provisional)
- **Required for Version 1.0**: NLP Narrative Classification Model
- **Planned analytical output**: Streamlit Narrative Drivers Page

### RQ6. Narrative persistence and reactivation
**How long do major Messi and Ronaldo narrative instances persist after their originating events, and which later events are associated with their reactivation?**
*This turns the project from a simple event dashboard into a narrative-lifecycle investigation.*

**Supports**:
- **Required for Version 1.0**: Narrative instance dimension (`dim_narrative_instance`, provisional)
- **Planned for Version 1.x**: Narrative lifecycle mart (`mart_narrative_lifecycle`, provisional)
- **Planned for Version 1.x**: Narrative reactivation-event bridge (`bridge_narrative_reactivation_event`, provisional)

### RQ7. Sarcasm and derision versus ordinary sentiment
**How do conclusions based on standard target-specific valence differ from conclusions produced when sarcasm, mockery, derogatory aliases, and derision are modelled explicitly?**
*This is important because football banter often breaks generic sentiment models.*

**Supports**:
- **Required for Version 1.0**: NLP Football Discourse Model (Derision/Sarcasm evaluation slices)
- **Required for Version 1.0**: Discourse model comparison mart (`mart_discourse_model_comparison`, provisional)
- **Planned analytical output**: Streamlit Reaction Profile

### RQ8. Channel baseline effects
**How do observed reactions within versioned channel strata differ from baselines calculated only from data preceding the event under analysis?**
*The point is not merely to show which channel stratum is most negative, but which channel stratum changed unusually relative to itself. Calculating baselines only from preceding data prevents future leakage.*

**Supports**:
- **Required for Version 1.0**: Versioned channel-stratum dimension (`dim_channel_stratum_version`, provisional)
- **Required for Version 1.0**: Channel shift decomposition mart (baseline metrics)
- **Planned analytical output**: Streamlit Channel Baseline Explorer

### RQ9. Matched-event comparison
**Across explicitly matched event strata, do observed reactions toward Messi and Ronaldo differ in magnitude, narrative distribution, derision rate, or recovery time?**
*Examples: major individual award wins, international tournament eliminations, moves to non-European leagues, age-35-plus performances, high-profile penalty misses.*

**Supports**:
- **Planned for Version 1.x**: Comparison stratum dimension (`dim_comparison_stratum`, provisional)
- **Planned for Version 1.x**: Matched-event case-control mart (`mart_matched_event_case_control`, provisional)
- **Planned analytical output**: Streamlit Cohort Comparison Dashboard

## Exploratory Research Question

### RQ10. Algorithmic surfacing versus chronological discussion
**How does YouTube’s relevance-ranked comment surface differ from the chronological comment stream in sentiment, derision, toxicity, narrative concentration, and author concentration?**
*This should remain a separate auxiliary investigation and never be mixed into the main time-series analysis. Comparisons must hold the video set and retrieval period constant wherever possible.*

**Supports**:
- **Planned for Version 1.x**: Comment sample-observation bridge (`bridge_comment_sample_observation`, provisional)
- **Planned for Version 1.x**: Relevance surface audit mart (`mart_relevance_surface_audit`, provisional)

---

## Relationship to the Project
Every downstream artefact—including sampling policies, warehouse schemas, NLP models, analytical marts, dashboard components, and metric definitions—must demonstrably support one or more research questions defined in this document. Every Version 1.0 artefact must support at least one research question designated as required for Version 1.0. Features supporting deferred or exploratory questions remain out of scope until the corresponding release phase.

*(Note: A formal traceability matrix mapping every research question to its source data, warehouse grains, NLP outputs, and final dashboard metrics will be maintained separately as the implementation matures).*
