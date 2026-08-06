# Streamlit Product Specification

The dashboard must guide the user through a structured causal investigation, shifting the focus from simple sentiment reporting to audience and narrative mechanics. 

Every chart should have:
- Question answered
- Metric definition
- Sample size
- Coverage & Exclusions
- Confidence or uncertainty
- Interpretation note

## 6-Step Investigation Flow

### Step 1: Detect a shift
*Example*: "Negative derision increased 32 percentage points after the event."
- **Focus**: Identify the magnitude and direction of the reaction shift using the multidimensional reaction profile (Valence, Sarcasm, Derision) and standardised reaction metrics.

### Step 2: Decompose the audience
*Example*: "58% of additional negative comments came from newly observed users. 42% came from returning users."
- **Focus**: Distinguish between an existing audience changing its behaviour and newly observed users entering the conversation.

### Step 3: Examine channel origin
*Example*: "Initial growth was concentrated in Ronaldo-focused channels. Mainstream-channel discussion increased 14 hours later."
- **Focus**: Measure the cross-channel inflow rate and apply channel-adjusted sentiment to separate the persistent channel framing from the actual audience response.

### Step 4: Measure existing-user change
*Example*: "Returning users became only 4 percentage points more negative. Most of the overall shift came from audience composition."
- **Focus**: Utilise the symmetric tone decomposition to formally prove whether the shift is driven by tone changes within communities or by a changing channel mix.

### Step 5: Identify the narrative
*Example*: "The dominant narrative was award legitimacy, not match performance."
- **Focus**: Expose the specific contextual `narrative_instance` driving the discourse, determining the narrative conversion rate and mainstream diffusion rate.

### Step 6: Compare a matched event
*Example*: "Compared with a similar Ronaldo award event under the `INDIVIDUAL_AWARD_WIN` stratum, Messi’s negative reaction was larger but returned to baseline more quickly."
- **Focus**: Compare the findings against an explicitly matched event cohort, openly highlighting the remaining contextual differences rather than relying on opaque matching scores.
