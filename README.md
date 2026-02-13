# DR4SS-NP

**DR4SS-NP** is an internal AFSC R package that provides a standardized, reusable framework for building a Stock Synthesis data file for North Pacific groudnfish. The package wraps a Shiny application used to pull all applicable data.

This repository is maintained under **afsc-assessments/DR4SS-NP** and is intended for **internal NOAA/AFSC analytical use**.

> ⚠️ **Data Access Notice**  
> This package connects to password-protected AFSC and AKFIN databases. It is not intended for public deployment.

---

## What this package does
use**, not a centralized Shiny server

---

## Installation

### From GitHub (internal use)

```r
install.packages("remotes")
remotes::install_github("afsc-assessments/DR4SS-NP")
```

---

## Running the dashboard

Once installed, launch the dashboard with:

```r
library(DR4SS-NP)
launch_DR4SS()
```

This will start the Shiny application in your local R session.

---

## Credentials and database access

The dashboard requires access to AFSC and AKFIN databases.

- Credentials are managed using the **keyring** package
- On first use, if credentials are not found, the app will prompt you to enter them
- Credentials are stored securely in your system credential store (not in the repo)

Required keyring services:
- `afsc`
- `akfin`

No passwords are written to disk or saved in plaintext.

---

## Requirements

### R packages

Core dependencies:
```r
datasets 
  data.table
  devtools
  dplyr
  fishmethods 
  flextable
  forcats 
  FSA
  ggplot2
  gnm  
  graphics 
  grDevices 
  grid
  gt
  keyring
  lubridate 
  magrittr 
  methods
  misty
  mgcv 
  nlme 
  nlstools
  officer 
  pdftools
  pivottabler 
  purr
  qgam
  r4ss
  readr
  readxl 
  remotes
  reshape2
  RODBC 
  scales
  sizeMat 
  sp
  ss3diags 
  stats
  stringr
  swo
  tabulizer
  tibble 
  tidyr 
  tidyverse 
  usethis 
  utils
  vcd
  vcdExtra
```

Optional (recommended):
```r
keyring           # secure credential storage
shinycssloaders   # loading spinners
```

Install missing packages with:
```r
install.packages(c(
  "shiny", "ggplot2", "bslib", "dplyr", "lubridate",
  "keyring", "shinycssloaders"
))
```

---

## Intended workflow

1. Install and load the package
2. Run `launch_DR4SS()`
3. Set species, dates, region, and gear filters
4. Click **Pull data only** to query databases
5. Click **Render plots** to generate visualizations
6. Use tabbed panels to explore results

This separation avoids unnecessary recomputation and supports iterative in-season analysis.

---

## Package structure (high level)

- `R/` – helper functions for data access and plotting
- `inst/shiny/` – Shiny application code
- `launch_DR4SS()` – wrapper to start the app

The Shiny app is intentionally bundled inside the package to ensure consistent behavior across analysts.

---

## Security and data sensitivity
- This package should only be used on approved systems
- Do not deploy to public Shiny servers without removing sensitive data access

---

## Disclaimer

This software is provided **as-is** for internal scientific and management support.  
It is not an official NOAA product and carries no warranty or guarantee of support.

---

## Maintainer

Steve Barbeaux Steve.barbeaux@noaa.gov  
Questions, issues, or enhancements should be coordinated within the AFSC assessment community.


## Write-up: `fit_age_predictor()` and `predict_age_from_lf()`

This document describes the statistical framework used to convert
fishery length-frequency (LF) data into predicted ages.

------------------------------------------------------------------------

## 1) Model Overview

The workflow has two stages:

1.  **fit_age_predictor()**
    -   Fits statistical models for expected length-at-age using:
        -   A survey-based Q3 backbone model
        -   A fishery quarter-specific delta model
    -   Builds empirical age priors from fishery-aged samples
2.  **predict_age_from_lf()**
    -   Uses Bayes' rule to compute posterior age probabilities
    -   Returns:
        -   Expected age compositions
        -   Posterior distributions
        -   Integer ages (MAP or sampled)

------------------------------------------------------------------------

## 2) Notation

Let:

-   L = observed length (cm)
-   A = age (integer)
-   A_G = plus-grouped age
-   S ∈ {F, M}
-   q ∈ {1,2,3,4} (quarter)
-   y = year
-   k = spatial stratum (AREA_K)
-   n = count of fish represented by an LF row

Plus-group definition:

A_G = min(max(A, 0), A_max)

------------------------------------------------------------------------

## 3) Survey Backbone Model (Q3)

Survey observations are treated as quarter 3 data.

The expected length-at-age model:

μ_Q3(a,s,y,k) = E\[L \| A_G=a, S=s, Y=y, K=k, q=3\]

Fitted using a GAM:

L = β0 + βS(s) + fS(a;s) + bY(y) + bK(k) + ε

Where: - fS(a;s) is a smooth of age by sex - bY(y) and bK(k) are random
effects - ε is residual error

------------------------------------------------------------------------

## 4) Fishery Quarter Delta Model

Fishery data occur in all quarters. Length-at-age in quarter q is:

μ_q(a,s,y,k) = μ_Q3(a,s,y,k) + δ_q(a,s,y,k)

For fishery observations:

Δ = L_obs − μ_Q3

Delta model:

Δ = αQ(q) + gQ(a;q) + αS(s) + uY(y) + uK(k) + η

Where: - gQ(a;q) is a quarter-specific smooth of age - uY(y), uK(k) are
random effects - η is residual error

------------------------------------------------------------------------

## 5) Age Priors

Empirical priors are constructed from fishery-aged samples.

### Pooled prior (area × sex)

π_pool(a \| k,s) = N\_{k,s,a} / Σ_a N\_{k,s,a}

### Global prior (sex)

π_glob(a \| s) = N\_{s,a} / Σ_a N\_{s,a}

### Cell prior (year × quarter × area × sex)

π_cell(a \| y,q,k,s) = N\_{y,q,k,s,a} / Σ_a N\_{y,q,k,s,a}

If N_cell ≥ min_n\_cell:

π(a \| y,q,k,s) = w \* π_cell(a \| y,q,k,s) + (1 − w) \* π_base(a \|
k,s)

where w = prior_mix.

Otherwise π_base is used alone.

All priors are normalized.

------------------------------------------------------------------------

## 6) Likelihood Model

Length-at-age is modeled as:

L \| a,y,q,k,s \~ Normal( μ_q(a,s,y,k), σ_q² )

σ_q = sqrt( σ_survey² + σ_delta² )

Likelihood:

ℒ(a) = φ(L ; μ_q(a,s,y,k), σ_q)

------------------------------------------------------------------------

## 7) Posterior Distribution

Using Bayes' rule:

p(a \| L,y,q,k,s) ∝ ℒ(a) × π(a \| y,q,k,s)

Normalized:

p(a \| ...) = ℒ(a) π(a) / Σ_a ℒ(a) π(a)

If numerical failure occurs, a uniform posterior is used.

------------------------------------------------------------------------

## 8) Outputs

### posterior_rows

Posterior vector for each unique (YEAR, QUARTER, AREA_K, SEX, LENGTH)

### row_age

MAP estimate: Â = argmax_a p(a \| L,...)

Sampling: Â \~ Categorical(p(a \| L,...))

### agecomp

Expected counts:

E\[N_a\] = n × p(a \| L,...)

Aggregated over lengths:

E\[N_a(y,q,k,s)\] = Σ_L n(L) p(a \| L,...)

------------------------------------------------------------------------

## 9) Interpretation

-   The survey backbone anchors the age-length relationship.
-   The delta model adjusts for seasonal growth differences.
-   Empirical priors stabilize predictions in sparse strata.
-   The framework is fully Bayesian at the prediction stage.

