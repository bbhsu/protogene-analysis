# protogene-analysis

This repo contains the code to identify a yeast strain as beneficial, neutral,
or deleterious based on colony size measurements.

## analysis_pipeline.m

This script contains the code to identify the effect of a genetic perturbation
in a yeast strain based on colony size measurements.

Input data for this script are from tables in a MySQL database.

This pipeline uses various functions in the ```sql_functions``` folder to
perform the calculations and statistical analysis.

The pipeline contains colony size and growth rate analysis. However, the workflow described below only covers the colony size analysis.

### Workflow
1. Clean up data by removing values and fixing sample swaps.
2. Calculate fitness by taking the normalized colony size at the time point where
colony size stops increasing.
3. Calculate fitness statistics (mean, median).
4. Perform statistical tests between the target and control strain fitnesses.
5. Correct p-values for multiple testing hypothesis using q-values.
6. Calculate the effect size thresholds as the 5th and 95th percentile of the control strain fitness.
7. Classify yeast strains as beneficial/neutral/deleterious based on q-value threshold and effect size.

## upload_raw_cs_to_db.m

This script contains the code to bulk upload raw colony sizes to a MySQL database.
