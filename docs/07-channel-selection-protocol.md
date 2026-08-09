# Channel Selection Protocol

**Version**: 1.0

This document dictates how channels are objectively discovered, evaluated, and selected for the Football Narrative Observatory. It serves as the bridge between the target population and the analytical channel strata, ensuring that channel membership does not suffer from subjective survivorship or confirmation bias.

## 1. Frame Membership (The Constructed Panel)
To avoid opaque algorithmic ranking biases, the observable candidate-channel frame is restricted to a **Constructed Panel** compiled from pre-specified, version-controlled external registries. 
The use of a Constructed Panel does not eliminate selection bias; rather, it introduces transparent, institutional coverage bias.

Every external source contributing to the Constructed Panel must declare:
- Exact source name (e.g., "Wikipedia List of English Football Broadcasters")
- Source version or snapshot date
- Retrieval/archive date
- Source-record identifier
- Documented mapping procedure converting the source entry to a YouTube Channel ID

Channels are eligible to be members of the frame only if their provenance to one or more of these approved external sources is documented.

## 2. General Channel Eligibility
General eligibility establishes baseline parameters for inclusion in the study.
- **Language**: English language eligibility is strictly evaluated at the video level (and subsequently the comment level). A channel is not globally excluded for publishing multi-lingual content, provided it publishes qualifying English videos.

## 3. Event-Channel Participation Eligibility
"Active" status is not a global frame requirement (which would introduce severe survivorship bias if historical channels are excluded for being dormant today). Channel activity is strictly **event-relative**. 
A channel is eligible for an event if it produced qualifying content within the event-specific observation windows defined by the Sampling Methodology. 
If it did not, it is excluded from that specific event analysis but remains in the Constructed Panel.

## 4. Classification Eligibility
Following event participation, channels are assessed for strata mapping:
- **Type Classification Availability**: Requires sufficient identity signals to classify channel type. Institutional channels (e.g., `CLUB_MEDIA`) are retained to provide a comparative baseline against independent creators.
- **Player Focus Classification Availability**: Requires $\ge 10$ videos in the 90-day pre-event reference period.
If evidence is insufficient, the axis is marked `UNCLASSIFIED`.

## 5. Decomposition Eligibility
Only channels with fully resolved composite strata (e.g., `BROAD_REACH_PUBLISHER` × `MIXED_FOCUS`) are eligible for stratified headline decompositions. Channels marked `UNCLASSIFIED` on either axis remain eligible for raw, un-decomposed aggregate volumes.

## 6. Architecture & Versioning
Frame definitions are versioned as immutable snapshots. 
- The frame version (e.g. `RESEARCH_V1`, `PIPELINE_PILOT`) controls which channels populate the downstream analytical engine.
- The 7 pilot channels are currently restricted to `PIPELINE_PILOT` frame configurations and receive zero automatic membership in `RESEARCH_V1`.
