# Project Vision

## Who might use the platform?

The platform is designed for **Researchers, Analysts, and Sports Media Professionals** who need transparent, sample-bounded analysis of online football discourse.

It is also designed for **Data and Analytics Engineers** evaluating reproducible ingestion, dimensional modelling, lineage, metric contracts, and analytical serving patterns.

## What investigative problem does it solve?

When aggregate sentiment changes online, it is often unclear what actually happened. The problem can be decomposed into several factors:
1. Aggregate sentiment may change because the same communities changed tone.
2. It may also change because different channels or commenters entered the observed sample.
3. Individual comments may simultaneously praise one player and attack another.
4. Sarcasm and football slang can make generic sentiment labels unreliable.

This platform solves the problem of disentangling these factors by:
1. Identifying whether observed reaction shifts are associated with returning authors, newly observed authors, or changes in the sampled channel mix.
2. Decomposing sentiment shifts into within-channel tone changes vs. changes in the channel composition of the sample.
3. Resolving targets explicitly, so that praise for one player is not conflated with derision for another.

## Why is ordinary sentiment analysis insufficient?

Ordinary sentiment analysis assumes a single, global sentiment for a given block of text. In football discourse, comments frequently contrast players (e.g., "Messi's pass was incredible, much better than Penaldo"). A standard sentiment model might average this out to "neutral," missing the targeted praise and targeted derision. Standard sentiment analysis also struggles with football-specific banter, sarcasm, and derogatory nicknames. Our approach requires models that can identify specific entities, evaluate stance toward each entity individually, and confidently abstain when the text is ambiguous or low-confidence.

## What makes the project distinctive?

- **Symmetric Decomposition**: We mathematically separate within-channel tone shifts from changes in the composition of the observed sample.
- **Matched Event Framework**: We don't just look at random points in time; we compare structurally similar events (e.g., both players missing a high-profile penalty, or winning an international tournament).
- **Target-Specific Granularity**: Every opinion is bound to a specific entity, text version, and model version.
- **Uncertainty-Aware Analysis**: Low-confidence entity resolution, ambiguous sentiment, incomplete ingestion, and weak event associations are surfaced explicitly rather than silently included.
- **Methodology First**: The project prioritises research design, limitations documentation, and explicit metric definitions over merely aggregating large amounts of API data.

## What would a successful result look like?

Success is defined across three dimensions:

- **Analytical success**: The system answers at least one matched-event investigation with transparent sampling, uncertainty, and coverage.
- **Engineering success**: The result is reproducible from raw payloads through warehouse transformations and versioned analytical marts.
- **Product success**: A Streamlit user can trace every headline metric back to its definition, sample size, source coverage, and limitations.

A successful result is a reproducible matched-event investigation that explains how observed discourse changed within the documented sample. It should distinguish within-channel tone shifts from changes in sampled channel composition, compare returning and authors newly observed within the sample without overstating lifetime behaviour, and analyse opinions separately for each player. The final Streamlit application should expose only validated metrics and clearly display sample coverage, uncertainty, exclusions, and methodological limitations. Every result must be traceable through a versioned data pipeline from raw API payloads to analytical marts.
