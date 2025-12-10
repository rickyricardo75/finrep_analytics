# DQ & Troubleshooting — Phase-1

## Common Issues
- **Date warnings (“Could not infer format”)**  
  Harmless; parser fell back to flexible mode. Add explicit `%d.%m.%Y`/`%Y-%m-%d` if needed.

- **Calendar parse < 90% → quarantine**  
  Check delimiter/encoding; validate header aliases; fix config and re-run Move-5.

- **Missing required → quarantine**  
  Add needed aliases; verify `required:` list is correct for that file type.

- **Duplicate loads**  
  Expected when ingesting the same period via different formats (e.g., PDF fixture). 
  For production, define dedup rule per file.

## Quick Checks
- Counts: `python check_src_counts.py`  
- Date ranges: one-liner in CMD cookbook  
- Calendar headers: `audit_calendars.py`
