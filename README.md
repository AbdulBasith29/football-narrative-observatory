# Messi vs Ronaldo Narrative Observatory

The Messi vs Ronaldo Narrative Observatory is a data platform that decomposes observed reaction changes into within-community tone shifts, channel-composition changes, returning-author behaviour, newly observed participation, and context-specific narrative movement. It evaluates targeted discourse toward Lionel Messi and Cristiano Ronaldo within explicitly matched event cohorts.

> **Methodological Contract**: The platform analyses observed discourse within a documented sample of selected YouTube channels, videos, event windows, and comments.

It will **not** claim to represent:
* all YouTube users;
* all Messi or Ronaldo fans;
* lifetime author behaviour;
* complete global public opinion;
* causal effects without qualification.

Findings apply *only* to the documented sample of selected English-language YouTube videos and comments.

## Project Overview

This repository contains the architecture, data models, and analytical tools designed to study the lifecycle of digital narratives surrounding the two defining football players of their generation. We focus on specific, matched football events (e.g., individual award wins, international eliminations) to understand how discussions are framed, rather than simply measuring overall sentiment.

## Research Motivation

Generic sentiment analysis fails when applied to football discourse because it cannot handle sarcasm, derision, or multi-entity comparisons ("Player A was amazing, but Player B is a fraud"). Furthermore, shifts in public discourse are often driven not by fans changing their minds, but by different fan communities participating at different times. We aim to separate true opinion shifts from audience-composition effects.

## Primary Investigation Questions

Our primary guiding question is:

> How do observed online reactions to Messi and Ronaldo change around comparable football events, and how much of each change is associated with shifts within existing channel audiences versus changes in the composition of the sampled discussion?

For the full list of our research questions, see [Research Questions](docs/01-research-questions.md).

## Methodological Principles

- **Target-Specific Opinions**: We evaluate sentiment toward Messi and Ronaldo separately, even when they appear in the same comment.
- **Symmetric Decomposition**: We separate changes in tone from changes in the mix of channels sampled.
- **Matched Events**: We compare similar types of events between the two players rather than taking arbitrary cross-sections of time.
- **Documented Uncertainty**: Our models are allowed to abstain (e.g., `LOW_CONFIDENCE`, `AMBIGUOUS_TARGET`), and metrics explicitly account for this uncertainty.

## Architecture Diagram

*(Diagram coming soon. See `diagrams/system-architecture.mmd`)*

## Current Project Status

**Phase 1C — Research Universe & Video Discovery Pipeline**
Phase 1C establishes the channel selection protocol, warehouse schema migrations (V001–V006), and a resumable video discovery service supporting channel uploads and search fallback with deterministic query hashing and quota budgeting. Temporary integration-test data has been verified in `FOOTBALL_NARRATIVE_TEST`. No Version 1.0 research comment sample has been ingested yet.

## Repository Navigation

- `docs/` - Core methodology, design documents, and architecture decisions (ADRs).
- `diagrams/` - Mermaid diagrams illustrating system models and workflows.
- `config/` - Configuration definitions for sampling, channels, events, and taxonomies.
- `ingestion/` - Data ingestion pipelines and API integrations.
- `transformations/` - dbt models for the layered data warehouse.
- `models/` - NLP inference and entity resolution models.
- `streamlit_app/` - The final data presentation and investigation dashboard.
- `scripts/` - Auxiliary automation and CI/CD tools.
- `tests/` - System, data quality, and unit tests.
- `notebooks/` - Exploratory data analysis and feasibility spikes.

## Planned Implementation Phases

- **Phase 0**: Repository and methodology
- **Phase 1**: Feasibility spike
- **Phase 2**: Physical warehouse design
- **Phase 3**: Chronological ingestion
- **Phase 4**: NLP evaluation
- **Phase 5**: First investigation
- **Phase 6**: Streamlit product

## Limitations

- The dataset is restricted to English-language YouTube videos and comments.
- Sampling is subject to YouTube API availability, pagination limits, and ranking effects.
- Findings are vulnerable to the deletion of content and the unavailability of historical user identifiers.
- For a comprehensive list, see [Limitations and Ethics](docs/10-limitations-and-ethics.md).

## Reproducibility Instructions
 
1. **Environment Setup**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Or on Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. **Configuration**:
   Copy `.env.example` to `.env` and configure credentials (`SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`, `YOUTUBE_API_KEY`).
3. **Database Bootstrap**:
   Bootstrap database objects, migrations V001–V006, and sync config dimensions:
   ```bash
   python scripts/setup_snowflake.py [optional-6-digit-totp-passcode]
   ```
4. **Run Test Suite**:
   Execute the full automated offline test suite:
   ```bash
   pytest tests/
   ```
