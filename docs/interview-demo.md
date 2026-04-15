# Interview Demo Script

## Open with the business problem

"Healthcare data is fragmented across providers, payers, labs, and referrals. That creates duplicate patient identities, low-trust reporting, and compliance risk because nobody can prove what happened to the data."

## Walk through the platform

1. "I generate intentionally messy healthcare feeds to simulate real ingestion problems: inconsistent DOB formats, missing MRNs, duplicate records across systems, and uneven contact data."
2. "The cleaning layer standardizes patient fields, normalizes phone and ZIP values, and flags unsafe records before anything reaches analytics."
3. "The deduplication layer uses explainable matching rules, not a black-box model. I can show exactly why two records merged using MRN, email plus DOB, or name plus DOB plus ZIP and phone."
4. "The warehouse is built in DuckDB, so the entire platform runs locally at zero infrastructure cost and still supports SQL marts for patient 360 views and platform monitoring."
5. "Every major stage writes to an audit trail and a metrics layer, which gives me compliance evidence plus observability for row counts, invalid DOB rates, and orphaned fact rows."
6. "The Streamlit dashboard is the operator view. It lets me show platform health, duplicate clusters, and the modeled warehouse from one interface."

## Strong close

"The point of the project is that I did not just clean a CSV. I built infrastructure that makes messy healthcare data safe, clean, and usable end to end."
