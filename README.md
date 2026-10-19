# Job Radar fetcher

Daily job-scan script for a personal Job Radar page. It reads open roles from employer job systems (Workday, SmartRecruiters, Oracle HCM, Phenom, iCIMS, SuccessFactors) and multi-company boards (Hipo, Workable, BestJobs, Himalayas).

It only sends HTTP requests to public job sites and writes JSON files.

## Daily snapshot

`.github/workflows/snapshot.yml` runs every morning on GitHub Actions:

```
python3 job_radar_fetch.py --snapshot out/snapshot.json --prev prev.json
```

and force-pushes `snapshot.json` to the `data` branch:
https://raw.githubusercontent.com/cuscus555/job-radar/data/snapshot.json

The snapshot holds every open job from the built-in sources that passes the location and title filters, all open job URLs, per-source status, and `closedIfMissing` (job systems that were read in full today). It needs no private inputs. Run it by hand from the Actions tab with "Run workflow".

## Local mode

```
python3 job_radar_fetch.py --db-dir <jobs dir> --out <out dir> [--companies companies.json] [--watchlist watchlist.json] [--no-details]
python3 job_radar_fetch.py --db-dir <jobs dir> --detect <website> ...
```
