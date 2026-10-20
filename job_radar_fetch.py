#!/usr/bin/env python3
"""Job Radar fetcher.

Reads open jobs straight from employers' job systems and a few trusted boards,
re-checks jobs already stored on the Job Radar page, and writes two files for
the scheduled scan to act on:

  candidates.json  open jobs not yet stored (with a description snippet)
  recheck.json     verdict for every stored job that is still active
  sources.json     per-source status (count or error)

Usage:
  python3 job_radar_fetch.py --db-dir <dir with ArtifactData out_dir JSON files> --out <dir>
  python3 job_radar_fetch.py --snapshot snapshot.json [--prev old_snapshot.json]

--snapshot needs no stored jobs, watchlist or registry. It writes every open job from
the built-in sources that passes the location and title filters (senior roles kept),
plus the list of all open URLs and which job systems were read successfully. It is
meant to run on a schedule (GitHub Actions) and publish plain data for the scan.
"""
import argparse, concurrent.futures as cf, datetime as dt, glob, hashlib, html, json, os, re, sys, time
import urllib.parse

try:
    import requests
except ImportError:  # pragma: no cover
    os.system(f"{sys.executable} -m pip install -q requests --break-system-packages >/dev/null 2>&1")
    import requests

UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
      "Accept-Language": "en-US,en;q=0.9,ro;q=0.8"}
TODAY = dt.date.today()
MAX_AGE_DAYS = 45
CLUJ = re.compile(r"cluj|jucu|flore[sș]ti|apahida|baciu", re.I)
REMOTE = re.compile(r"\bremote\b|la distan[tț][aă]|work from home|wfh", re.I)

# Titles that are never a fit (applied before any detail fetch). Kept conservative:
# anything technical stays in and the scan rates it.
EXCLUDE_TITLE = re.compile(
    r"\b(sales|account manager|key account|accountant|accounts payable|accounts receivable|payroll|recruit|talent acquisition|"
    r"human resources|\bhr\b|people partner|controller|controlling|credit|collection|treasury|tax|financ(e|ial) (analyst|specialist|controller)|"
    r"customer service|customer support|call center|order (fulfil|management|administrator)|logistic|purchas|procurement|buyer|"
    r"operator|assembly|warehouse|driver|facility|cleaning|receptionist|legal|lawyer|counsel|marketing|communications specialist|"
    r"demand planning|planner|government relations|office manager|administrative|secretar|translator|"
    r"working student|intern\b|internship|stagiu|practic(a|ă)|student|werkstudent|praktik|ausbildung|"
    r"sap (key user|fico|sd|mm|hcm|s/4hana sales|functional)|key user|service desk|help ?desk)\b", re.I)

SENIOR_TITLE = re.compile(r"\b(senior|sr\.?|lead|principal|staff|manager|head of|director|architect|vp|chief|expert)\b", re.I)
JUNK_REMOTE = re.compile(r"future position|talent pool|copy of|ai trainer|code reviewer|evaluation engineer|annotat|freelance|"
                         r"\bcontract\b|part[- ]time|commission|unpaid|volunteer|ios developer|android developer|unity|game", re.I)
TECH_TITLE = re.compile(r"embedded|firmware|fpga|asic|hardware|electronic|test|qa\b|quality|validation|verification|devops|\bsre\b|reliability|"
                        r"cloud|infrastructure|platform|network|security|cyber|software|developer|programmer|engineer|full[- ]?stack|back[- ]?end|"
                        r"front[- ]?end|python|c\+\+|\bc#|java|data|integration|automation|scada|plc|robot|\bml\b|\bai\b|machine learning|linux|systems|"
                        r"application|administrat|\badmin\b|sysadmin|threat|\bsoc\b|\biam\b|identity|architect|autosar|aurosar|technolog|telecom|"
                        r"re[tț]e(a|le)?\b|electronist|(technical|tech|it|application|app|production|product|desktop|2nd[- ]level|second[- ]level|l[123]) support|"
                        r"support (engineer|analyst|specialist)", re.I)
# "IT" as a word, matched case-sensitively so the English word "it" does not count
IT_TITLE = re.compile(r"\bIT\b")


def is_tech(title):
    return bool(TECH_TITLE.search(title or "") or IT_TITLE.search(title or ""))

COMPANY_KEYS = [
    (r"analog devices|\badi\b", "analog-devices", "Analog Devices (ADI)"),
    (r"bosch", "bosch", "Bosch Engineering Center Cluj"),
    (r"porsche engineering", "porsche-engineering", "Porsche Engineering Romania"),
    (r"\bmhp\b", "mhp", "MHP"),
    (r"bmw ?tech ?works", "bmw-techworks", "BMW TechWorks Romania"),
    (r"emerson", "emerson", "Emerson"),
    (r"vertiv", "vertiv", "Vertiv"),
    (r"ntt ?data", "ntt-data", "NTT DATA Romania"),
    (r"marsh|mmc ?tech|mercer|oliver wyman|guy carpenter", "marsh-mclennan", "Marsh McLennan"),
    (r"\bing\b|ing hubs", "ing-hubs", "ING Hubs Romania"),
    (r"\bpwc\b|pricewaterhouse", "pwc", "PwC Romania"),
    (r"arobs", "arobs", "AROBS"),
    (r"mindsoft", "mindsoft", "Mindsoft"),
]


def slug(s, n=70):
    s = html.unescape(s or "").lower()
    s = re.sub(r"\(f/m/d\)|\(m/w/d\)|\(m/f/d\)|\(f/m/x\)|\bromania\b|\bsrl\b|\bs\.r\.l\.?", " ", s)
    s = (s.replace("ș", "s").replace("ş", "s").replace("ț", "t").replace("ţ", "t").replace("ă", "a").replace("â", "a").replace("î", "i"))
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s[:n].strip("-")


def company_key(name):
    for pat, key, _ in COMPANY_KEYS:
        if re.search(pat, name or "", re.I):
            return key
    name = re.sub(r"\b(gmbh|ltd|limited|inc|llc|srl|s\.r\.l|bv|b\.v|ag|sa|plc|corp(oration)?|company)\b\.?", " ", name or "", flags=re.I)
    return slug(name, 40)


def company_display(name):
    for pat, _, disp in COMPANY_KEYS:
        if re.search(pat, name or "", re.I):
            return disp
    return name


def norm_title(t):
    t = html.unescape(t or "").lower()
    t = re.sub(r"\(.*?\)|@\s*[\w ]+$|-\s*(junior|senior|mid|consultant|senior consultant|manager|senior manager|extern)\b.*$", " ", t)
    t = re.sub(r"\b(junior|senior|sr|jr|mid|medior|lead|principal|staff|ii|iii|i)\b", " ", t)
    return re.sub(r"[^a-z0-9]+", " ", t).strip()


def pair_key(company, title):
    return company_key(company) + "|" + norm_title(title)


def strip_html(h, limit=None):
    h = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", h or "")
    h = re.sub(r"(?i)<br\s*/?>|</p>|</li>|</h\d>|</div>", "\n", h)
    t = html.unescape(re.sub(r"<[^>]+>", " ", h))
    t = re.sub(r"[ \t\r\f\v]+", " ", t)
    t = re.sub(r"\n\s*\n+", "\n", t).strip()
    return t[:limit] if limit else t


def parse_date(s):
    if not s:
        return None
    s = str(s).strip()
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return dt.date.fromisoformat(m.group(1))
    for fmt in ("%d-%m-%Y", "%d.%m.%Y", "%b %d, %Y", "%d %b %Y", "%B %d, %Y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            pass
    m = re.search(r"posted (today|yesterday|(\d+)\+? days? ago)", s, re.I)
    if m:
        if m.group(1).lower() == "today":
            return TODAY
        if m.group(1).lower() == "yesterday":
            return TODAY - dt.timedelta(days=1)
        return TODAY - dt.timedelta(days=int(m.group(2)))
    try:
        return dt.datetime.strptime(s, "%b %d, %Y").date()
    except Exception:
        return None


def years_required(text):
    found = []
    for m in re.finditer(r"(?:minimum|min\.?|at least|cel pu[tț]in|minim)?\s*(\d{1,2})\s*(?:\+|-\s*\d{1,2}|to \d{1,2})?\s*(?:\+\s*)?(?:years|yrs|ani)", text or "", re.I):
        n = int(m.group(1))
        if 0 < n < 25:
            found.append(n)
    return min(found) if found else None


def flags(text):
    t = text or ""
    return {
        "germanRequired": bool(re.search(r"\b(german|deutsch|limba german[aă])\b", t, re.I)),
        "yearsRequired": years_required(t),
    }


def job(company, title, location, url, source, source_id, posted=None, mode=None, remote_scope=None, desc=None, extra=None):
    location = re.sub(r"\s+", " ", html.unescape(location or "")).strip()
    is_remote = bool(REMOTE.search(" ".join([location, mode or "", title or ""])))
    if remote_scope:
        region = remote_scope
    elif CLUJ.search(location):
        region = "Cluj"
    elif is_remote and re.search(r"romania|\bro\b", location, re.I):
        region = "Remote Romania"
    else:
        region = None
    d = {
        "company": company_display(company), "companyRaw": company, "title": html.unescape(title or "").strip(),
        "location": location, "mode": mode or ("Remote" if is_remote else None), "region": region,
        "url": url, "source": source, "sourceId": str(source_id) if source_id is not None else None,
        "posted": posted.isoformat() if isinstance(posted, dt.date) else (posted or None), "desc": desc,
    }
    if extra:
        d.update(extra)
    return d


# Watchlist companies are flagged as targets (their senior roles are kept). The list is read at
# run time from --watchlist (JSON {"companies": [{"name": ...}]}); without it, the companies above are used.
TARGET_KEYS = set()


def load_watchlist(path):
    try:
        d = json.load(open(path))
    except Exception as e:
        print(f"watchlist not loaded: {e}", file=sys.stderr)
        return
    d = d.get("data", d)
    for c in d.get("companies", []):
        name = (c.get("name") or "").strip()
        if not name:
            continue
        if not any(re.search(p, name, re.I) for p, _, _ in COMPANY_KEYS):
            first = slug(name.split()[0], 30)
            if len(first) >= 3 or re.fullmatch(r"[a-z]\d", first):
                COMPANY_KEYS.append((r"\b" + first.replace("-", "[ -]?") + r"\b", first, name))
        TARGET_KEYS.add(company_key(name))

SESSION = requests.Session()
SESSION.headers.update(UA)


def get(url, **kw):
    kw.setdefault("timeout", 40)
    return SESSION.get(url, **kw)


def post(url, **kw):
    kw.setdefault("timeout", 40)
    return SESSION.post(url, **kw)


# ---------------------------------------------------------------- sources
def src_workday(company, host, tenant, site, search_texts):
    base = f"https://{host}/wday/cxs/{tenant}/{site}"
    seen, out = set(), []
    for q in search_texts:
        offset = 0
        while offset < 400:
            r = post(base + "/jobs", json={"appliedFacets": {}, "limit": 20, "offset": offset, "searchText": q},
                     headers={"Accept": "application/json", "Content-Type": "application/json"})
            r.raise_for_status()
            j = r.json()
            posts = j.get("jobPostings", [])
            for p in posts:
                path = p.get("externalPath")
                if not path or path in seen:
                    continue
                seen.add(path)
                loc = p.get("locationsText") or ""
                if not CLUJ.search(loc):
                    try:
                        info = get(base + path, headers={"Accept": "application/json"}).json().get("jobPostingInfo", {})
                        loc = "; ".join(x for x in [info.get("location") or ""] + list(info.get("additionalLocations") or []) if x) or loc
                    except Exception:
                        pass
                out.append(job(company, p.get("title"), loc, f"https://{host}/en-US/{site}{path}",
                               f"{company} careers (Workday)", path, parse_date(p.get("postedOn")),
                               extra={"_detail": ("workday", base + path)}))
            offset += 20
            if offset >= (j.get("total") or 0) or not posts:
                break
    return out


def src_smartrecruiters(company, sr_company):
    out, offset = [], 0
    while True:
        r = get(f"https://api.smartrecruiters.com/v1/companies/{sr_company}/postings", params={"country": "ro", "limit": 100, "offset": offset})
        r.raise_for_status()
        j = r.json()
        for p in j.get("content", []):
            loc = p.get("location", {})
            locs = ", ".join(x for x in [loc.get("city"), loc.get("region"), "Romania"] if x)
            mode = "Remote" if loc.get("remote") else ("Hybrid" if loc.get("hybrid") else None)
            out.append(job(company, p.get("name"), locs, f"https://jobs.smartrecruiters.com/{sr_company}/{p['id']}",
                           f"{company} careers (SmartRecruiters)", p["id"], parse_date((p.get("releasedDate") or "")[:10]), mode,
                           extra={"_detail": ("smartrecruiters", f"https://api.smartrecruiters.com/v1/companies/{sr_company}/postings/{p['id']}"),
                                  "experienceLevel": (p.get("experienceLevel") or {}).get("label")}))
        offset += 100
        if offset >= j.get("totalFound", 0):
            break
    return out


def src_oracle(company, host, site, keyword="Cluj"):
    u = f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    out, offset = [], 0
    while True:
        finder = f'findReqs;siteNumber={site},facetsList=LOCATIONS,limit=100,offset={offset},keyword="{keyword}",sortBy=POSTING_DATES_DESC'
        r = get(u, params={"onlyData": "true", "expand": "requisitionList.secondaryLocations", "finder": finder}, headers={"Accept": "application/json"})
        r.raise_for_status()
        it = (r.json().get("items") or [{}])[0]
        reqs = it.get("requisitionList", [])
        for q in reqs:
            locs = [q.get("PrimaryLocation") or ""] + [s.get("Name", "") for s in (q.get("secondaryLocations") or [])]
            mode = {"ORA_ON_SITE": "On-site", "ORA_HYBRID": "Hybrid", "ORA_REMOTE": "Remote"}.get(q.get("WorkplaceTypeCode") or "", None)
            out.append(job(company, q.get("Title"), "; ".join(l for l in locs if l),
                           f"https://{host}/hcmUI/CandidateExperience/en/sites/{site}/job/{q['Id']}",
                           f"{company} careers (Oracle)", q["Id"], parse_date(q.get("PostedDate")), mode,
                           extra={"_detail": ("oracle", (host, site, q["Id"]))}))
        offset += 100
        if offset >= (it.get("TotalJobsCount") or 0) or not reqs:
            break
    return out


def src_phenom_marsh():
    out, frm = [], 0
    while True:
        body = {"lang": "en_eu", "deviceType": "desktop", "country": "eu", "pageName": "search-results", "ddoKey": "refineSearch",
                "sortBy": "Most recent", "subsearch": "", "from": frm, "jobs": True, "counts": True, "all_fields": ["category", "country", "state", "city"],
                "size": 50, "clearAll": False, "jdsource": "facets", "isSliderEnable": False, "pageId": "page20", "siteType": "external",
                "keywords": "", "global": True, "selected_fields": {"city": ["Cluj-Napoca"]}}
        r = post("https://careers.marsh.com/widgets", json=body, headers={"Content-Type": "application/json"})
        r.raise_for_status()
        rs = r.json().get("refineSearch", {})
        jobs = rs.get("data", {}).get("jobs", [])
        for x in jobs:
            jid = x.get("jobId") or x.get("reqId")
            url = x.get("applyUrl") if False else f"https://careers.marsh.com/eu/en/job/{jid}"
            out.append(job("Marsh McLennan", x.get("title"), ", ".join(filter(None, [x.get("city"), x.get("country")])), url,
                           "Marsh McLennan careers", x.get("jobSeqNo") or jid, parse_date((x.get("postedDate") or "")[:10]),
                           desc=strip_html(x.get("descriptionTeaser") or "", 1500)))
        frm += 50
        if frm >= (rs.get("totalHits") or 0) or not jobs:
            break
    return out


def src_porsche():
    url = "https://porsche-beesite-production-gjb.app.beesite.de/search/"
    payload = {"LanguageCode": "EN", "SearchParameters": {"FirstItem": 1, "CountItem": 2000,
               "Sort": [{"Criterion": "PublicationStartDate", "Direction": "DESC"}],
               "MatchedObjectDescriptor": ["ID", "PositionTitle", "PositionURI", "PositionLocation.CityName", "PositionLocation.CountryName",
                                           "ParentOrganizationName", "PublicationStartDate", "CareerLevel.Name"]}, "SearchCriteria": []}
    r = get(url, params={"data": json.dumps(payload)}, headers={"Origin": "https://jobs.porsche.com", "Referer": "https://jobs.porsche.com/"})
    r.raise_for_status()
    out = []
    for it in r.json()["SearchResult"]["SearchResultItems"]:
        d = it["MatchedObjectDescriptor"]
        locs = d.get("PositionLocation") or []
        cities = "/".join((l.get("CityName") or l.get("CountryName") or "") for l in locs if isinstance(l, dict))
        org = d.get("ParentOrganizationName") or ""
        if not (CLUJ.search(cities) or ("romania" in cities.lower() and REMOTE.search(d.get("PositionTitle", "")))):
            continue
        if not re.search(r"porsche engineering|mhp", org, re.I):
            continue
        out.append(job(org, d.get("PositionTitle"), cities.replace(" /", "/"), d.get("PositionURI"), "jobs.porsche.com", d.get("ID"),
                       parse_date(d.get("PublicationStartDate")),
                       extra={"_detail": ("html", d.get("PositionURI")), "postedNote": "Porsche keeps long-running ads open; listing means open"}))
    return out


def src_rmk_nttdata():
    out, start = [], 0
    while start < 500:
        r = get("https://careers.nttdata.ro/search/", params={"q": "", "locationsearch": "Cluj", "startrow": start})
        r.raise_for_status()
        h = r.text
        rows = re.findall(r'<tr class="data-row">(.*?)</tr>', h, re.S)
        for row in rows:
            m = re.search(r'class="jobTitle-link"[^>]*href="([^"]+)"[^>]*>([^<]+)<', row)
            if not m:
                continue
            path, title = m.group(1), m.group(2)
            dm = re.search(r'class="jobDate[^"]*"[^>]*>\s*([^<]+?)\s*<', row)
            lm = re.search(r'class="jobLocation[^"]*"[^>]*>\s*([^<]+?)\s*<', row)
            company = "BMW TechWorks Romania" if path.startswith("/bmwtechworks/") else "NTT DATA Romania"
            jid = re.search(r"/(\d+)/?$", path)
            out.append(job(company, title, ((lm.group(1).strip() if lm else "") or "Cluj, RO"), "https://careers.nttdata.ro" + path,
                           f"{company} careers (SuccessFactors)", jid.group(1) if jid else path, parse_date(dm.group(1) if dm else None),
                           extra={"_detail": ("html", "https://careers.nttdata.ro" + path)}))
        tot = re.search(r"of\s*<b>(\d+)</b>", h)
        start += 25
        if not rows or not tot or start >= int(tot.group(1)):
            break
    return out


def src_icims_arobs():
    out = []
    for pr in range(0, 6):
        r = get("https://careers-arobs.icims.com/jobs/search", params={"ss": "1", "in_iframe": "1", "pr": pr})
        r.raise_for_status()
        h = r.text
        blocks = re.split(r'class="row"', h)
        found = 0
        for b in blocks:
            m = re.search(r'href="(https://careers-arobs\.icims\.com/jobs/(\d+)/[^"?]+/job)[^"]*"[^>]*title="([^"]+)"', b) or \
                re.search(r'href="(https://careers-arobs\.icims\.com/jobs/(\d+)/[^"?]+/job)[^"]*"[^>]*>\s*(?:<[^>]+>\s*)*([^<]{3,150})', b)
            if not m:
                continue
            title = re.sub(r"^\d+\s*-\s*", "", m.group(3)).strip()
            if title.lower() == "job title":
                h2 = re.search(r"<h3[^>]*>\s*([^<]+)</h3>", b) or re.search(r'<h2[^>]*>\s*([^<]+)</h2>', b)
                title = h2.group(1).strip() if h2 else title
            loc = re.search(r"(RO-[A-Z]{1,3}-[^<\n]+|Cluj[^<\n]*|Remote[^<\n]*)", b)
            found += 1
            out.append(job("AROBS", title, loc.group(1).strip() if loc else "", m.group(1), "AROBS careers (iCIMS)", m.group(2),
                           None, extra={"_detail": ("html", m.group(1) + "?in_iframe=1")}))
        if not found:
            break
    # dedupe by id
    uniq = {}
    for j in out:
        uniq.setdefault(j["sourceId"], j)
    res = list(uniq.values())

    def locate(j):
        try:
            t = strip_html(get(j["url"] + "?in_iframe=1").text)
            locs = sorted(set(re.findall(r"RO-[A-Z]{1,3}-[A-Za-z\- ]+|\bRemote\b|Cluj-Napoca|Cluj", t)))
            j["location"] = ", ".join(locs)[:120]
            if CLUJ.search(j["location"]):
                j["region"] = "Cluj"
            elif re.search(r"remote", j["location"], re.I):
                j["region"], j["mode"] = "Remote Romania", "Remote"
        except Exception:
            pass
        return j
    with cf.ThreadPoolExecutor(6) as ex:
        return list(ex.map(locate, res))


HIPO_CATS = ["IT-Software", "IT-Hardware", "Inginerie", "Controlul-calitatii", "Telecomunicatii"]


def src_hipo():
    out = []
    for cat in HIPO_CATS:
        r = get(f"https://www.hipo.ro/locuri-de-munca/cautajob/{cat}/Cluj-Napoca/")
        r.raise_for_status()
        for blk in re.split(r'<div class="job-item', r.text)[1:]:
            m = re.search(r'title="([^"]+)"\s+class="job-title"\s+href="(/locuri-de-munca/locuri_de_munca/(\d+)/([^/"]+)/[^"]*)"', blk)
            if not m:
                continue
            title, path, jid, comp_slug = m.groups()
            comp = re.search(r'alt="([^"]+)"', blk)
            text = strip_html(blk)
            date = re.search(r"(\d{2}-\d{2}-\d{4})", text)
            loc = "Cluj-Napoca" if re.search(r"cluj", text, re.I) else ("Remote" if re.search(r"remote", text, re.I) else text[:60])
            mode = "Hybrid" if re.search(r"hybrid", text, re.I) else ("Remote" if re.search(r"\bremote\b", text, re.I) else None)
            out.append(job(html.unescape(comp.group(1)) if comp else comp_slug.replace("-", " "), title, loc, "https://www.hipo.ro" + path,
                           "Hipo", jid, parse_date(date.group(1)) if date else None, mode,
                           extra={"_detail": ("html", "https://www.hipo.ro" + path)}))
    uniq = {}
    for j in out:
        uniq.setdefault(j["sourceId"], j)
    return list(uniq.values())


def src_mindsoft():
    r = get("https://mindsoft.ro/en/cariere/")
    r.raise_for_status()
    out = []
    for m in re.finditer(r'href="(https://mindsoft\.ro/en/careers_archive/[^"]+)"[^>]*>(.*?)</a>', r.text, re.S):
        title = strip_html(m.group(2))
        if not title or len(title) > 120:
            continue
        out.append(job("Mindsoft", title, "Sibiu", m.group(1), "Mindsoft careers", m.group(1), None,
                       extra={"_detail": ("html", m.group(1))}))
    uniq = {}
    for j in out:
        uniq.setdefault(j["url"], j)
    return list(uniq.values())


HIMALAYAS_Q = ["embedded", "firmware", "fpga", "hardware", "test automation", "qa engineer", "devops", "network engineer",
               "security engineer", "full stack", "python", "c++", "junior", "data engineer", "systems engineer"]


def src_himalayas():
    out = []
    for q in HIMALAYAS_Q:
        r = get("https://himalayas.app/jobs/api/search", params={"q": q, "country": "Romania"})
        if r.status_code != 200:
            continue
        for j in r.json().get("jobs", []):
            restr = [str(x) for x in (j.get("locationRestrictions") or [])]
            if restr and not any(re.search(r"romania|europe|\beu\b|emea|worldwide", x, re.I) for x in restr):
                continue
            pub = dt.datetime.fromtimestamp(int(j.get("pubDate") or 0)).date() if j.get("pubDate") else None
            exp = dt.datetime.fromtimestamp(int(j.get("expiryDate") or 0)).date() if j.get("expiryDate") else None
            if exp and exp < TODAY:
                continue
            scope = "Remote Romania" if restr and all(re.search(r"romania", x, re.I) for x in restr) else "Remote EU"
            out.append(job(j.get("companyName"), j.get("title"), ", ".join(restr) or "Worldwide", j.get("applicationLink") or j.get("guid"),
                           "Himalayas (remote)", j.get("guid"), pub, "Remote", remote_scope=scope,
                           desc=strip_html(j.get("description") or "", 2500),
                           extra={"seniorityTag": ", ".join(j.get("seniority") or [])}))
    uniq = {}
    for j in out:
        uniq.setdefault(j["sourceId"], j)
    return list(uniq.values())


# ---------------------------------------------------------------- multi-company boards
def src_workable_search():
    out, seen = [], set()
    for params in ({"location": "Cluj-Napoca"}, {"location": "Romania", "workplace": "remote"}):
        token = None
        for _ in range(10):
            q = dict(params)
            if token:
                q["pageToken"] = token
            r = get("https://jobs.workable.com/api/v1/jobs", params=q, headers={"Accept": "application/json"})
            r.raise_for_status()
            j = r.json()
            for x in j.get("jobs", []):
                if x["id"] in seen:
                    continue
                seen.add(x["id"])
                loc = x.get("location") or {}
                locs = ", ".join(filter(None, [loc.get("city"), loc.get("countryName") or loc.get("country")]))
                wp = (x.get("workplace") or "").lower()
                scope = None
                if wp == "remote" and not CLUJ.search(locs):
                    scope = "Remote Romania" if re.search(r"romania", locs, re.I) else None
                out.append(job((x.get("company") or {}).get("title", ""), x.get("title"), locs, x.get("url"), "Workable", x["id"],
                               parse_date(x.get("created")), {"remote": "Remote", "hybrid": "Hybrid", "on_site": "On-site"}.get(wp),
                               remote_scope=scope,
                               desc=strip_html((x.get("requirementsSection") or "") + "\n" + (x.get("description") or ""), 2500)))
            token = j.get("nextPageToken")
            if not token or not j.get("jobs"):
                break
    return out


BESTJOBS_PAGES = ["it-software", "inginerie", "telecomunicatii", "embedded", "junior", "qa", "devops", "hardware", "automatizari"]


def src_bestjobs():
    out, seen = [], set()
    for page in BESTJOBS_PAGES:
        r = get(f"https://www.bestjobs.eu/locuri-de-munca-in-cluj-napoca/{page}")
        if r.status_code != 200:
            continue
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
        if not m:
            continue
        items = (json.loads(m.group(1)).get("props", {}).get("pageProps", {}).get("jobListCardsFromServer") or {}).get("items", [])
        for x in items:
            if x["id"] in seen or (x.get("state") and x["state"] != "active"):
                continue
            seen.add(x["id"])
            locs = "; ".join(l.get("name", "") for l in (x.get("locations") or []))
            mode = "Remote" if x.get("workplaceJob") else None
            out.append(job(x.get("companyName"), x.get("title"), locs or "Cluj-Napoca", f"https://www.bestjobs.eu/loc-de-munca/{x['slug']}",
                           "BestJobs", x["id"], None, mode, extra={"_detail": ("html", f"https://www.bestjobs.eu/loc-de-munca/{x['slug']}")}))
    return out


# ---------------------------------------------------------------- company registry (small companies, startups)
ATS_PATTERNS = [
    ("workday", r"https?://([a-z0-9-]+)\.(wd\d+)\.myworkdayjobs\.com/(?:[a-z]{2}-[A-Z]{2}/)?([A-Za-z0-9_-]+)"),
    ("teamtailor", r"https?://([a-z0-9-]+)\.teamtailor\.com"),
    ("recruitee", r"https?://([a-z0-9-]+)\.recruitee\.com"),
    ("lever", r"jobs\.(?:eu\.)?lever\.co/([A-Za-z0-9-]+)"),
    ("greenhouse", r"(?:boards|job-boards)(?:\.eu)?\.greenhouse\.io/(?:embed/job_board\?for=)?([A-Za-z0-9-]+)"),
    ("workable", r"apply\.workable\.com/([A-Za-z0-9-]+)"),
    ("personio", r"https?://([a-z0-9-]+)\.jobs\.personio\.(?:de|com)"),
    ("breezy", r"https?://([a-z0-9-]+)\.breezy\.hr"),
    ("smartrecruiters", r"(?:jobs|careers)\.smartrecruiters\.com/([A-Za-z0-9-]+)"),
    ("bamboohr", r"https?://([a-z0-9-]+)\.bamboohr\.com"),
    ("ashby", r"jobs\.ashbyhq\.com/([A-Za-z0-9.%-]+)"),
]
CAREER_LINK = re.compile(r"career|cariere|jobs?\b|joburi|join[- ]us|alatura|angaj|open[- ]positions|we.?re[- ]hiring|work[- ]with[- ]us", re.I)


def detect_company(website):
    """Find a company's careers page and job system from its website."""
    res = {"website": website, "careersUrl": None, "ats": None, "slug": None}
    try:
        r = get(website, timeout=25)
        pages = [(r.url, r.text)]
        base = r.url
        links = []
        for href, text in re.findall(r'<a[^>]+href="([^"#]+)"[^>]*>(.*?)</a>', r.text, re.S | re.I):
            t = strip_html(text)
            if CAREER_LINK.search(href) or CAREER_LINK.search(t):
                links.append(urllib.parse.urljoin(base, href))
        for guess in ("/careers", "/cariere", "/jobs", "/en/careers", "/ro/cariere"):
            links.append(urllib.parse.urljoin(base, guess))
        tried = set()
        for l in links:
            if l in tried or len(tried) >= 6:
                continue
            tried.add(l)
            try:
                rr = get(l, timeout=20)
            except Exception:
                continue
            if rr.status_code == 200:
                pages.append((rr.url, rr.text))
                if not res["careersUrl"] and urllib.parse.urlparse(rr.url).path not in ("", "/"):
                    res["careersUrl"] = rr.url
        for url, h in [(u, u) for u, _ in pages] + pages:
            for ats, pat in ATS_PATTERNS:
                m = re.search(pat, h)
                if m:
                    res["ats"] = ats
                    res["slug"] = m.group(1)
                    if ats == "workday":
                        res["slug"] = {"tenant": m.group(1), "wd": m.group(2), "site": m.group(3)}
                    res["careersUrl"] = res["careersUrl"] or url
                    return res
        res["ats"] = "page" if res["careersUrl"] else None
    except Exception as e:
        res["error"] = str(e)[:120]
    return res


def _company_job(c, title, location, url, sid, posted=None, mode=None, desc=None):
    location = location or ""
    scope = None
    if not CLUJ.search(location):
        city = (c.get("city") or "").lower()
        if (not location.strip() or re.search(r"romania|remote", location, re.I)) and city.startswith("cluj") and not re.search(r"remote", location, re.I):
            location = (location + " (company is in Cluj)").strip()
        elif re.search(r"remote", location + " " + (mode or ""), re.I) and re.search(r"romania|europe|\beu\b|emea|anywhere|worldwide", location, re.I):
            scope = "Remote Romania" if re.search(r"romania", location, re.I) else "Remote EU"
    j = job(c["name"], title, location, url, f"{c['name']} careers", sid, posted, mode, remote_scope=scope, desc=desc)
    if not j["region"] and "company is in Cluj" in location:
        j["region"] = "Cluj"
    return j


def fetch_registry_company(c):
    ats, slug, out = c.get("ats"), c.get("slug"), []
    if ats == "teamtailor":
        r = get(f"https://{slug}.teamtailor.com/jobs.rss"); r.raise_for_status()
        for it in re.findall(r"<item>(.*?)</item>", r.text, re.S):
            t = re.search(r"<title>(.*?)</title>", it, re.S); l = re.search(r"<link>(.*?)</link>", it, re.S)
            loc = re.search(r"<tt:location>.*?<tt:city>(.*?)</tt:city>", it, re.S) or re.search(r"<tt:city>(.*?)</tt:city>", it, re.S)
            pd = re.search(r"<pubDate>(.*?)</pubDate>", it)
            remote = re.search(r"<remoteStatus>(.*?)</remoteStatus>", it)
            out.append(_company_job(c, html.unescape(strip_html(t.group(1))) if t else "", loc.group(1) if loc else "", l.group(1).strip() if l else c.get("careersUrl"),
                                    l.group(1).strip() if l else None, None, "Remote" if remote and "fully" in remote.group(1) else None,
                                    strip_html(it, 2000)))
    elif ats == "recruitee":
        r = get(f"https://{slug}.recruitee.com/api/offers/"); r.raise_for_status()
        for o in r.json().get("offers", []):
            out.append(_company_job(c, o.get("title"), ", ".join(filter(None, [o.get("city"), o.get("country")])) + (" Remote" if o.get("remote") else ""),
                                    o.get("careers_url"), o.get("id"), parse_date(o.get("published_at")), "Remote" if o.get("remote") else None,
                                    strip_html((o.get("requirements") or "") + (o.get("description") or ""), 2500)))
    elif ats == "lever":
        r = get(f"https://api.lever.co/v0/postings/{slug}", params={"mode": "json"}); r.raise_for_status()
        for o in r.json():
            cat = o.get("categories") or {}
            out.append(_company_job(c, o.get("text"), cat.get("location") or ", ".join(cat.get("allLocations") or []), o.get("hostedUrl"), o.get("id"),
                                    dt.datetime.fromtimestamp((o.get("createdAt") or 0) / 1000).date() if o.get("createdAt") else None,
                                    {"remote": "Remote", "hybrid": "Hybrid", "onsite": "On-site"}.get(o.get("workplaceType") or ""),
                                    strip_html(o.get("descriptionPlain") or o.get("description") or "", 2500)))
    elif ats == "greenhouse":
        r = get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"); r.raise_for_status()
        for o in r.json().get("jobs", []):
            out.append(_company_job(c, o.get("title"), (o.get("location") or {}).get("name"), o.get("absolute_url"), o.get("id"), parse_date(o.get("updated_at"))))
    elif ats == "workable":
        r = post(f"https://apply.workable.com/api/v3/accounts/{slug}/jobs", json={"query": "", "location": [], "department": [], "worktype": [], "remote": []})
        r.raise_for_status()
        for o in r.json().get("results", []):
            loc = o.get("location") or {}
            out.append(_company_job(c, o.get("title"), ", ".join(filter(None, [loc.get("city"), loc.get("country")])) + (" Remote" if o.get("remote") else ""),
                                    f"https://apply.workable.com/{slug}/j/{o.get('shortcode')}/", o.get("shortcode"), parse_date(o.get("published")),
                                    "Remote" if o.get("remote") else None))
    elif ats == "personio":
        r = get(f"https://{slug}.jobs.personio.de/xml"); r.raise_for_status()
        for pos in re.findall(r"<position>(.*?)</position>", r.text, re.S):
            g = lambda tag: (re.search(fr"<{tag}>(.*?)</{tag}>", pos, re.S) or [None, ""])[1]
            pid = g("id")
            out.append(_company_job(c, html.unescape(g("name")), html.unescape(g("office")), f"https://{slug}.jobs.personio.de/job/{pid}", pid, parse_date(g("createdAt"))))
    elif ats == "breezy":
        r = get(f"https://{slug}.breezy.hr/json"); r.raise_for_status()
        for o in r.json():
            loc = o.get("location") or {}
            out.append(_company_job(c, o.get("name"), ", ".join(filter(None, [loc.get("city"), (loc.get("country") or {}).get("name") if isinstance(loc.get("country"), dict) else loc.get("country")])) + (" Remote" if loc.get("is_remote") else ""),
                                    o.get("url"), o.get("id"), parse_date(o.get("published_date")), "Remote" if loc.get("is_remote") else None))
    elif ats == "smartrecruiters":
        r = get(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings", params={"limit": 100}); r.raise_for_status()
        for p in r.json().get("content", []):
            loc = p.get("location", {})
            out.append(_company_job(c, p.get("name"), ", ".join(filter(None, [loc.get("city"), loc.get("country")])) + (" Remote" if loc.get("remote") else ""),
                                    f"https://jobs.smartrecruiters.com/{slug}/{p['id']}", p["id"], parse_date((p.get("releasedDate") or "")[:10])))
    elif ats == "bamboohr":
        r = get(f"https://{slug}.bamboohr.com/careers/list", headers={"Accept": "application/json"}); r.raise_for_status()
        for o in r.json().get("result", []):
            loc = o.get("location") or {}
            out.append(_company_job(c, o.get("jobOpeningName"), ", ".join(filter(None, [loc.get("city"), loc.get("state")])),
                                    f"https://{slug}.bamboohr.com/careers/{o.get('id')}", o.get("id")))
    elif ats == "ashby":
        r = get(f"https://api.ashbyhq.com/posting-api/job-board/{slug}"); r.raise_for_status()
        for o in r.json().get("jobs", []):
            out.append(_company_job(c, o.get("title"), (o.get("location") or "") + (" Remote" if o.get("isRemote") else ""), o.get("jobUrl"), o.get("id"),
                                    parse_date(o.get("publishedAt")), "Remote" if o.get("isRemote") else None, strip_html(o.get("descriptionPlain") or "", 2500)))
    elif ats == "workday" and isinstance(slug, dict):
        out = src_workday(c["name"], f"{slug['tenant']}.{slug['wd']}.myworkdayjobs.com", slug["tenant"], slug["site"], ["Cluj", "Romania"])
    elif ats == "page" and c.get("careersUrl"):
        r = get(c["careersUrl"]); r.raise_for_status()
        text = strip_html(r.text)
        windows, last = [], -1
        for m in re.finditer(r"(?i)\b(engineer|developer|programmer|tester|qa\b|devops|embedded|firmware|hardware|electronic|intern|junior|"
                             r"inginer|dezvoltator|programator|post(?:ul)?|position|opening|vacanc|hiring|angaj)", text):
            if m.start() < last:
                continue
            seg = text[max(0, m.start() - 120): m.start() + 220]
            windows.append(seg)
            last = m.start() + 220
        joined = "\n".join(windows)
        h = hashlib.sha1(re.sub(r"\d+", "", joined).encode()).hexdigest()[:16]
        PAGE_STATUS.append({"name": c["name"], "careersUrl": c["careersUrl"], "hash": h, "changed": h != c.get("pageHash"),
                            "excerpt": joined[:3000], "city": c.get("city")})
    for j in out:
        j["discovered"] = True
    return out


PAGE_STATUS = []


def src_registry(companies):
    out, errors = [], []

    def one(c):
        try:
            return fetch_registry_company(c), None
        except Exception as e:
            return [], f"{c.get('name')}: {type(e).__name__}"
    with cf.ThreadPoolExecutor(8) as ex:
        for jobs_, err in ex.map(one, [c for c in companies if c.get("ats")]):
            out += jobs_
            if err:
                errors.append(err)
    if errors:
        print("registry errors:", "; ".join(errors[:20]), file=sys.stderr)
    return out


SOURCES = [
    ("Analog Devices (Workday)", lambda: src_workday("Analog Devices", "analogdevices.wd1.myworkdayjobs.com", "analogdevices", "External", ["Romania", "Cluj"])),
    ("ING Hubs (Workday)", lambda: src_workday("ING Hubs Romania", "ing.wd3.myworkdayjobs.com", "ing", "ICSGBLCOR", ["Cluj"])),
    ("PwC (Workday)", lambda: src_workday("PwC Romania", "pwc.wd3.myworkdayjobs.com", "pwc", "Global_Experienced_Careers", ["Cluj"]) +
                      src_workday("PwC Romania", "pwc.wd3.myworkdayjobs.com", "pwc", "Global_Campus_Careers", ["Cluj"])),
    ("Bosch (SmartRecruiters)", lambda: src_smartrecruiters("Bosch", "BoschGroup")),
    ("Emerson (Oracle)", lambda: src_oracle("Emerson", "hdjq.fa.us2.oraclecloud.com", "CX_1")),
    ("Vertiv (Oracle)", lambda: src_oracle("Vertiv", "egup.fa.us2.oraclecloud.com", "CX")),
    ("Marsh McLennan (Phenom)", src_phenom_marsh),
    ("Porsche Engineering + MHP (jobs.porsche.com)", src_porsche),
    ("NTT DATA + BMW TechWorks (SuccessFactors)", src_rmk_nttdata),
    ("AROBS (iCIMS)", src_icims_arobs),
    ("Hipo Cluj lists", src_hipo),
    ("Workable (all companies, Cluj + remote Romania)", src_workable_search),
    ("BestJobs Cluj", src_bestjobs),
    ("Mindsoft", src_mindsoft),
    ("Himalayas remote", src_himalayas),
]


# ---------------------------------------------------------------- details
def fetch_detail(j):
    if j.get("desc"):
        return j
    kind, ref = j.get("_detail") or (None, None)
    try:
        if kind == "workday":
            r = get(ref, headers={"Accept": "application/json"})
            if r.status_code == 200:
                info = r.json().get("jobPostingInfo", {})
                locs = [info.get("location") or ""] + list(info.get("additionalLocations") or [])
                j["location"] = "; ".join(l for l in locs if l) or j["location"]
                if CLUJ.search(j["location"]):
                    j["region"] = "Cluj"
                j["desc"] = strip_html(info.get("jobDescription") or "", 3000)
                j["posted"] = j["posted"] or info.get("startDate")
                if info.get("remoteType"):
                    j["mode"] = info.get("remoteType")
        elif kind == "smartrecruiters":
            r = get(ref)
            if r.status_code == 200:
                sec = (r.json().get("jobAd") or {}).get("sections", {})
                j["desc"] = strip_html(" \n".join((sec.get(k) or {}).get("text", "") for k in ("jobDescription", "qualifications")), 3000)
        elif kind == "oracle":
            host, site, rid = ref
            r = get(f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails",
                    params={"expand": "all", "onlyData": "true", "finder": f'ById;Id="{rid}",siteNumber={site}'}, headers={"Accept": "application/json"})
            if r.status_code == 200 and r.json().get("items"):
                it = r.json()["items"][0]
                j["desc"] = strip_html((it.get("ExternalDescriptionStr") or "") + "\n" + (it.get("ExternalQualificationsStr") or ""), 3000)
        elif kind == "html":
            r = get(ref)
            if r.status_code == 200:
                t = strip_html(r.text)
                k = re.search(r"(requirements|qualifications|your profile|what you bring|what we expect|you have|cerin[tț]e|profil)", t, re.I)
                start = max(0, (k.start() - 400) if k else 0)
                j["desc"] = t[start:start + 3000]
    except Exception as e:
        j["descError"] = str(e)[:120]
    return j


# ---------------------------------------------------------------- stored jobs
def load_db(db_dir):
    jobs = []
    for f in glob.glob(os.path.join(db_dir, "**", "*.json"), recursive=True):
        try:
            d = json.load(open(f))
        except Exception:
            continue
        data = d.get("data", d)
        jid = d.get("id") or os.path.splitext(os.path.basename(f))[0]
        if "title" in data:
            jobs.append({"id": jid, **data})
    return jobs


def recheck(stored, fetched_by_url, fetched_ids, fetched_pairs):
    """Return verdicts for stored jobs that are still marked active."""
    out = []

    def one(s):
        url = s.get("url") or ""
        host = urllib.parse.urlparse(url).netloc.lower()
        pk = pair_key(s.get("company", ""), s.get("title", ""))
        v = {"id": s["id"], "title": s.get("title"), "company": s.get("company"), "status": s.get("status"), "url": url}
        if url in fetched_by_url or pk in fetched_pairs:
            return {**v, "verdict": "open", "evidence": "Listed on the employer's job system or Hipo today"}
        covered = {"jobs.porsche.com": "jobs.porsche.com", "careers.nttdata.ro": "careers.nttdata.ro",
                   "careers-arobs.icims.com": "AROBS careers", "careers.marsh.com": "Marsh careers", "jobs.smartrecruiters.com": "Bosch careers"}
        for dom, label in covered.items():
            if dom in host:
                return {**v, "verdict": "closed", "evidence": f"No longer listed on {label}"}
        if "myworkdayjobs.com" in host or "oraclecloud.com" in host:
            return {**v, "verdict": "closed", "evidence": "No longer listed on the employer's job system"}
        try:
            r = get(url, allow_redirects=True, timeout=30)
            t = r.text
            if r.status_code in (404, 410):
                return {**v, "verdict": "closed", "evidence": f"Posting page returns {r.status_code}"}
            if "hipo.ro" in host:
                if re.search(r"no longer active|nu mai este activ", t, re.I):
                    return {**v, "verdict": "closed", "evidence": "Hipo marks it no longer active"}
                return {**v, "verdict": "open", "evidence": "Hipo page still active"}
            if "builtin.com" in host:
                if re.search(r"this job was removed|job was removed", t, re.I):
                    return {**v, "verdict": "closed", "evidence": "Built In says the job was removed"}
                m = re.search(r"(?:Posted|Reposted)\s+(\d+)\s+Days?\s+Ago", t, re.I)
                if m and int(m.group(1)) <= MAX_AGE_DAYS:
                    return {**v, "verdict": "open", "evidence": f"Built In, posted {m.group(1)} days ago"}
                return {**v, "verdict": "unknown", "evidence": "Built In page has no recent posting date"}
            if "undelucram.ro" in host:
                m = re.search(r"(?:Expir[ăa]|Expires)\s*(\d{2}\.\d{2}\.\d{4})", t)
                if m:
                    exp = dt.datetime.strptime(m.group(1), "%d.%m.%Y").date()
                    return {**v, "verdict": "open" if exp >= TODAY else "closed", "evidence": f"undelucram expiry {exp.isoformat()}"}
                return {**v, "verdict": "unknown", "evidence": "undelucram page shows no expiry"}
            if "mindsoft.ro" in host:
                return {**v, "verdict": "closed", "evidence": "No longer on Mindsoft's careers page"}
            return {**v, "verdict": "unknown", "evidence": f"{host} cannot confirm whether the job is open"}
        except Exception as e:
            return {**v, "verdict": "unknown", "evidence": f"Fetch failed: {str(e)[:80]}"}

    todo = [s for s in stored if s.get("active") is not False]
    with cf.ThreadPoolExecutor(8) as ex:
        out = list(ex.map(one, todo))
    return out


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-dir")
    ap.add_argument("--snapshot", help="Write a public snapshot of open jobs to this file (no private inputs) and exit")
    ap.add_argument("--prev", help="Previous snapshot; descriptions of unchanged jobs are reused")
    ap.add_argument("--out", default=".")
    ap.add_argument("--no-details", action="store_true")
    ap.add_argument("--companies", help="JSON file: ArtifactData out_dir file of config/companies")
    ap.add_argument("--detect", nargs="+", help="Detect careers page and job system for these websites, print JSON, and exit")
    ap.add_argument("--watchlist", help="JSON file of target companies: {\"companies\": [{\"name\": ...}]}")
    a = ap.parse_args()
    if a.snapshot:
        return snapshot_main(a)
    if not a.db_dir:
        ap.error("--db-dir is required unless --snapshot is used")
    if a.watchlist:
        load_watchlist(a.watchlist)
    if not TARGET_KEYS:
        TARGET_KEYS.update(k for _, k, _ in COMPANY_KEYS)
    if a.detect:
        with cf.ThreadPoolExecutor(6) as ex:
            print(json.dumps(list(ex.map(detect_company, a.detect)), indent=1))
        return
    os.makedirs(a.out, exist_ok=True)

    sources, fetched = {}, []
    for name, fn in SOURCES:
        t0 = time.time()
        try:
            items = fn()
            sources[name] = {"ok": True, "count": len(items), "secs": round(time.time() - t0, 1)}
            fetched += items
        except Exception as e:
            sources[name] = {"ok": False, "error": f"{type(e).__name__}: {str(e)[:160]}"}

    if a.companies and os.path.exists(a.companies):
        d = json.load(open(a.companies))
        companies = d.get("data", d).get("companies", [])
        t0 = time.time()
        try:
            items = src_registry(companies)
            sources[f"Company registry ({len(companies)} companies)"] = {"ok": True, "count": len(items), "secs": round(time.time() - t0, 1)}
            fetched += items
        except Exception as e:
            sources["Company registry"] = {"ok": False, "error": str(e)[:160]}

    stored = load_db(a.db_dir)
    by_url = {j["url"]: j for j in fetched if j.get("url")}
    ids = {j["sourceId"] for j in fetched if j.get("sourceId")}
    pairs = {pair_key(j["company"], j["title"]) for j in fetched}
    verdicts = recheck(stored, by_url, ids, pairs)

    stored_urls = {s.get("url") for s in stored}
    stored_pairs = {pair_key(s.get("company", ""), s.get("title", "")) for s in stored}
    stored_ids = {s["id"] for s in stored}

    cands, seen_pairs, skipped = [], set(), {"location": 0, "title": 0, "stale": 0, "stored": 0, "duplicate": 0}
    for j in fetched:
        if not j.get("region"):
            skipped["location"] += 1
            continue
        if EXCLUDE_TITLE.search(j["title"]) or not is_tech(j["title"]) or \
                (j["region"] != "Cluj" and JUNK_REMOTE.search(j["title"])):
            skipped["title"] += 1
            continue
        target = company_key(j["company"]) in TARGET_KEYS
        if not target and (SENIOR_TITLE.search(j["title"]) or re.search(r"senior|manager|director|lead|executive", j.get("seniorityTag") or "", re.I)):
            skipped["senior_nontarget"] = skipped.get("senior_nontarget", 0) + 1
            continue
        pd = parse_date(j.get("posted")) if j.get("posted") else None
        if pd and (TODAY - pd).days > MAX_AGE_DAYS and j["source"] in ("Hipo", "Himalayas (remote)"):
            skipped["stale"] += 1
            continue
        pk = pair_key(j["company"], j["title"])
        cid = f"{company_key(j['company'])}__{slug(j['title'])}__{'cluj' if j['region'] == 'Cluj' else 'remote'}"
        if j["url"] in stored_urls or pk in stored_pairs or cid in stored_ids:
            skipped["stored"] += 1
            continue
        if pk in seen_pairs:
            skipped["duplicate"] += 1
            continue
        seen_pairs.add(pk)
        j["id"] = cid[:150]
        cands.append(j)

    # employer systems first, so the same role on Hipo is dropped as a duplicate above only when the employer copy came first
    if not a.no_details:
        with cf.ThreadPoolExecutor(8) as ex:
            cands = list(ex.map(fetch_detail, cands))
    for j in cands:
        j.update(flags((j.get("title") or "") + "\n" + (j.get("desc") or "")))
        j.pop("_detail", None)
        j.pop("companyRaw", None)
        if j.get("desc"):
            j["desc"] = j["desc"][:1800]

    json.dump(cands, open(os.path.join(a.out, "candidates.json"), "w"), ensure_ascii=False, indent=1)
    json.dump(verdicts, open(os.path.join(a.out, "recheck.json"), "w"), ensure_ascii=False, indent=1)
    json.dump(PAGE_STATUS, open(os.path.join(a.out, "pages.json"), "w"), ensure_ascii=False, indent=1)
    json.dump({"date": TODAY.isoformat(), "sources": sources, "skipped": skipped, "fetched": len(fetched),
               "candidates": len(cands), "stored": len(stored),
               "careersPagesChanged": [x["name"] for x in PAGE_STATUS if x["changed"]],
               "recheck": {v: sum(1 for x in verdicts if x["verdict"] == v) for v in ("open", "closed", "unknown")}},
              open(os.path.join(a.out, "sources.json"), "w"), indent=1)
    print(json.dumps(json.load(open(os.path.join(a.out, "sources.json"))), indent=1))


# ---------------------------------------------------------------- public snapshot
# Text in a job link -> source that lists every open job on that host. When that source was read
# successfully today and a stored job on the host is not in the snapshot, the job has closed.
CLOSED_IF_MISSING = [
    ("jobs.porsche.com", "Porsche Engineering + MHP (jobs.porsche.com)"),
    ("careers.nttdata.ro", "NTT DATA + BMW TechWorks (SuccessFactors)"),
    ("careers-arobs.icims.com", "AROBS (iCIMS)"),
    ("careers.marsh.com", "Marsh McLennan (Phenom)"),
    ("jobs.smartrecruiters.com/BoschGroup", "Bosch (SmartRecruiters)"),
    ("analogdevices.wd1.myworkdayjobs.com", "Analog Devices (Workday)"),
    ("ing.wd3.myworkdayjobs.com", "ING Hubs (Workday)"),
    ("pwc.wd3.myworkdayjobs.com", "PwC (Workday)"),
    ("hdjq.fa.us2.oraclecloud.com", "Emerson (Oracle)"),
    ("egup.fa.us2.oraclecloud.com", "Vertiv (Oracle)"),
    ("mindsoft.ro", "Mindsoft"),
]


def snapshot_main(a):
    TARGET_KEYS.update(k for _, k, _ in COMPANY_KEYS)
    prev = {}
    if a.prev and os.path.exists(a.prev):
        try:
            prev = {j["url"]: j for j in json.load(open(a.prev)).get("jobs", []) if j.get("url") and j.get("desc")}
        except Exception as e:
            print(f"previous snapshot not loaded: {e}", file=sys.stderr)

    sources, fetched = {}, []
    for name, fn in SOURCES:
        t0 = time.time()
        try:
            items = fn()
            sources[name] = {"ok": True, "count": len(items), "secs": round(time.time() - t0, 1)}
            fetched += items
        except Exception as e:
            sources[name] = {"ok": False, "error": f"{type(e).__name__}: {str(e)[:160]}"}

    jobs, seen, skipped = [], set(), {"location": 0, "title": 0, "stale": 0, "duplicate": 0}
    for j in fetched:
        if not j.get("region"):
            skipped["location"] += 1
            continue
        if EXCLUDE_TITLE.search(j["title"]) or not is_tech(j["title"]) or \
                (j["region"] != "Cluj" and JUNK_REMOTE.search(j["title"])):
            skipped["title"] += 1
            continue
        pd = parse_date(j.get("posted")) if j.get("posted") else None
        if pd and (TODAY - pd).days > MAX_AGE_DAYS and j["source"] in ("Hipo", "Himalayas (remote)"):
            skipped["stale"] += 1
            continue
        pk = pair_key(j["company"], j["title"])
        if pk in seen:
            skipped["duplicate"] += 1
            continue
        seen.add(pk)
        j["id"] = f"{company_key(j['company'])}__{slug(j['title'])}__{'cluj' if j['region'] == 'Cluj' else 'remote'}"[:150]
        j["pairKey"] = pk
        j["senior"] = bool(SENIOR_TITLE.search(j["title"]) or re.search(r"senior|manager|director|lead|executive", j.get("seniorityTag") or "", re.I))
        old = prev.get(j.get("url"))
        if old and not j.get("desc"):
            j["desc"] = old["desc"]
        jobs.append(j)

    if not a.no_details:
        with cf.ThreadPoolExecutor(8) as ex:
            jobs = list(ex.map(fetch_detail, jobs))
    for j in jobs:
        j.update(flags((j.get("title") or "") + "\n" + (j.get("desc") or "")))
        j.pop("_detail", None)
        j.pop("companyRaw", None)
        if j.get("desc"):
            j["desc"] = j["desc"][:1800]

    ok = {n for n, v in sources.items() if v.get("ok")}
    out = {
        "date": TODAY.isoformat(),
        "generatedAt": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "sources": sources,
        "closedIfMissing": [h for h, n in CLOSED_IF_MISSING if n in ok],
        "openUrls": sorted({j["url"] for j in fetched if j.get("url")}),
        "skipped": skipped,
        "jobs": jobs,
    }
    tmp = a.snapshot + ".tmp"
    json.dump(out, open(tmp, "w"), ensure_ascii=False, indent=1)
    os.replace(tmp, a.snapshot)
    print(json.dumps({"date": out["date"], "jobs": len(jobs), "openUrls": len(out["openUrls"]),
                      "sourcesOk": len(ok), "sourcesFailed": [n for n in sources if n not in ok], "skipped": skipped}, indent=1))


if __name__ == "__main__":
    main()
