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
