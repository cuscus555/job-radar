# Job Radar fetcher

Daily job-scan script for a personal Job Radar page. It reads open roles from employer job systems (Workday, SmartRecruiters, Oracle HCM, Phenom, iCIMS, SuccessFactors), multi-company boards (Hipo, Workable, BestJobs, Himalayas) and a registry of smaller companies, and re-checks stored jobs.

It only sends HTTP requests to public job sites and writes JSON files to the `--out` directory.

```
python3 job_radar_fetch.py --db-dir <jobs dir> --out <out dir> [--companies companies.json] [--watchlist watchlist.json] [--no-details]
python3 job_radar_fetch.py --db-dir <jobs dir> --detect <website> ...
```

Outputs: `sources.json`, `recheck.json`, `candidates.json`, `pages.json`.
