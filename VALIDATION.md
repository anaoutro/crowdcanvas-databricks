# CrowdCanvas validation

Updated: 2026-10-05. All inputs are synthetic.

## Executed locally

- Fourteen portable flow-analysis tests passed.
- Fixture generation reconciles 314 rows = 294 accepted + 12 duplicates + 8 quarantined payloads.
- Twenty-seven accepted snapshots have delay greater than ten minutes. There are sixty windows, six incomplete windows and eighteen synthetic pressure windows.
- Editorial cover and case-board JPGs were rendered from editable HTML/CSS/SVG layouts and visually inspected.

## Pending platform execution

Databricks notebooks, Spark/Delta integration, actual watermark progress/finalization, privileges and SQL execution require a workspace. No actual streaming result or benchmark is claimed.

Repository syntax, metadata and local asset-link checks passed. Browser demonstrations passed desktop and mobile interaction checks with no runtime errors. The fixture generator reproduces the checked-in artifacts. GitHub Actions is prepared, not yet executed on GitHub.

