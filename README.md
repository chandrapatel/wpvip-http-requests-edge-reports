# WPVIP HTTP Requests Edge Reports

This tool turns WordPress VIP edge request logs (`http_requests_edge`) into monthly traffic reports. The reports are HTML pages you open in a browser. Each log entry is one HTTP request that reached the WPVIP edge network, including requests that the cache passed on to the origin.

It works for a single site and for a multisite network. See [Single sites and multisites](#single-sites-and-multisites).

For each month, the script writes two kinds of report:

- **Overview.** It covers every host in the logs: request volume, human and bot traffic, cache hit rate, bandwidth and response time. It also has a table that compares the sites and a crawler analysis.
- **Site report.** There is one for each site. It has the same overview, plus:
  - top URLs for each category
  - the slowest URLs
  - 404 and 5xx errors
  - referrers and countries
  - a crawler analysis for that site

The script uses only the Python standard library. It needs no extra packages.

## Requirements

- **Log shipping:** [WPVIP log shipping](https://docs.wpvip.com/logs/log-shipping/) must be enabled for HTTP request edge logs, delivered to an S3 bucket.
- **AWS access:** you need AWS credentials that can read that bucket, and the [AWS CLI](https://aws.amazon.com/cli/).
- **Python:** version 3.9 or newer. The script was tested with 3.9 and 3.14.
- **Disk space:** enough for one month of logs. A busy site or multisite can produce 60–100 GB of compressed logs a month. Check the size first, as shown in [Check log size on S3](#check-log-size-on-s3).

## Folder layout

The examples use `~/wpvip-logs` as the working folder. You can use any folder. The logs keep the same layout as in S3, so you sync them without renaming anything.

```
~/wpvip-logs/
├── http_requests_edge/                        Raw logs, same layout as S3. Safe to delete after processing.
│   └── 2026/
│       └── 08/
│           ├── 01/*.gz                        One folder per day.
│           └── 02/*.gz
└── reports/                                   Generated reports. Kept when logs are deleted.
    └── 2026-08/
        ├── report_2026-08.html                Overview of all hosts. Open this in a browser.
        ├── report_2026-08.json                All monthly data, for all hosts and each site.
        ├── sites/
        │   ├── report_2026-08_example.com.html    Full report for one site.
        │   └── …
        └── daily/                             One file per day, used to build the monthly data.
```

The script finds the `reports/` folder from the log path, and names each report folder `YYYY-MM`. For `~/wpvip-logs/http_requests_edge/2026/08/`, reports go to `~/wpvip-logs/reports/2026-08/`.

## One-time setup

1. Save your AWS credentials as a named profile, for example with `aws configure --profile wpvip-logs`.
2. Add these lines to your shell profile, such as `~/.zshrc`. Replace the values with your own, then open a new terminal.

   ```sh
   export WPVIP_AWS_PROFILE=wpvip-logs            # AWS profile with read access to the bucket
   export WPVIP_LOG_BUCKET=s3://your-bucket/logs  # Bucket, plus the optional prefix set in log shipping
   export WPVIP_LOGS=~/wpvip-logs                 # Local working folder
   ```

   The commands below pass the profile with `--profile "$WPVIP_AWS_PROFILE"`. This keeps your default AWS profile unchanged for other work.

3. Check Python with `python3 --version`.

## Monthly workflow

Replace `2026/10` with the month you want, in both the S3 path and the local path.

### Step 1: Download the logs

```sh
aws s3 sync --profile "$WPVIP_AWS_PROFILE" \
  "$WPVIP_LOG_BUCKET/http_requests_edge/2026/10/" \
  "$WPVIP_LOGS/http_requests_edge/2026/10/"
```

`s3 sync` downloads only the files you do not have yet. Run it again at any time to fetch new logs for the current month.

Sync one month at a time, as shown, rather than the whole `http_requests_edge/` folder. A full sync downloads every month in the bucket. It also downloads again any month you deleted locally to save space.

### Step 2: Generate the reports

Run this from the repo folder:

```sh
python3 process_logs.py "$WPVIP_LOGS/http_requests_edge/2026/10/" --workers 3
```

The script processes each day folder, writes a daily report, and then merges all daily reports into the monthly data. When it finishes, open `reports/2026-10/report_2026-10.html` in a browser. Each site name in the "Requests by Site" table links to that site's report.

#### Choosing the number of workers

`--workers 3` is safe on a machine with 16 GB of memory. Each worker processes one day at a time. A worker uses up to about 3.2 GB of memory for a busy day of about 34 million requests, so 3 workers use about 10 GB. That leaves room for the operating system and your other apps.

The figures in this section come from one busy site, measured on an Apple Silicon laptop. Your run time and memory use will differ with the size of your logs. They depend mostly on requests per day and on how many distinct URLs, search queries and user agents the logs contain. Use the table as a starting point, not a guarantee.

If your machine has more memory, raise the number for a faster run:

| Memory | Workers | Peak memory used | Time for a busy 31-day month |
| --- | --- | --- | --- |
| 16 GB | `--workers 3` | about 10 GB | about 1 h 45 min |
| 32 GB | `--workers 6` | about 20 GB | about 55 min |
| 64 GB or more | `--workers 12` | about 39 GB | about 30 min |

- **CPU cores also set a limit.** More workers than CPU cores won't make the run faster. On a Mac with performance and efficiency cores, count only the performance cores.
- **Log size changes the numbers.** A quieter site uses less memory and time per day, so it can run more workers on the same machine. A busier site, or one with heavy bot traffic that creates many distinct URLs and user agents, needs more memory per worker. In that case, use fewer workers.
- **Check on the first run.** Watch memory pressure in Activity Monitor, or use `top`. If the system starts swapping, stop the run with Ctrl+C and use fewer workers. Finished days keep their daily reports, so the next run continues where this one stopped.

### Running it again

You can run both steps again whenever you want. The script skips days that already have a daily report, so a second run only processes new work:

- A new day folder is processed.
- A day folder that gained files since its last report is processed again. This happens when you sync a day before it has finished.
- A day processed by an older version of the script is processed again, because the report format changed.
- Every other day is skipped.

To process every day again from the start, add `--force`.

To change only the look of the reports, use `--html-only`. It rebuilds every HTML file from the monthly JSON in a few seconds. It does not read the logs, so it works after you delete them.

### Deleting logs

After the monthly report exists, you can delete the month's log folder to free disk space. The reports stay in `reports/`. If you run the script again after deleting the logs, it rebuilds the monthly report from the daily reports.

## Script options

| Option | What it does |
| --- | --- |
| `--workers N` | Process N days at the same time. The default is 1. See [Choosing the number of workers](#choosing-the-number-of-workers). |
| `--force` | Process every day again, even days that already have a daily report. |
| `--html-only` | Rebuild the HTML reports from the monthly JSON. It does not read the logs. |
| `--output DIR` | Write reports to DIR instead of `reports/<month>/`. |

## Sharing reports

The HTML files contain everything a reader needs. The JSON files are working data for the script. To share a month, send the overview and the `sites/` folder together, because the pages link to each other:

```sh
cd "$WPVIP_LOGS/reports"
zip -r report_2026-10.zip 2026-10/report_2026-10.html 2026-10/sites
```

The reports load Chart.js from cdnjs.cloudflare.com. Without internet access, the charts stay blank, but every table and number still works.

## How the reports count traffic

### Traffic and cache

- **Human** is requests that WPVIP classifies as `People`.
- **Bot / Crawler** is every other WPVIP class, such as `AI Search Crawler` or `Fetcher`.
- **Unclassified** is requests that WPVIP did not classify. The reports show these separately, not as bots, because they can be a large share of traffic.
- **Cache hit rate** follows the [WPVIP `x-cache` definitions](https://docs.wpvip.com/caching/page-cache/http-headers/added-by-vip/). `HIT`, `STALE` and `UPDATING` count as served from cache. `MISS`, `EXPIRED`, `BYPASS` and `REVALIDATED` count as served by the origin.

### Single sites and multisites

The script groups requests by host name, not by WordPress site ID. It removes `www.` and port numbers, so `www.example.com` and `example.com:443` count as `example.com`. A host gets its own site report when it has at least 0.1% of all requests. Smaller hosts, such as typo domains and IP addresses, appear only in the "Other hosts" table of the overview.

What you get depends on how the WordPress install is set up:

| Setup | Reports |
| --- | --- |
| Single site | One site report, plus the overview. The overview shows nearly the same numbers as the site report, so open the site report for URL details. |
| Multisite with a domain or subdomain per site, such as `site-a.com` and `blog.example.com` | One site report per host, plus an overview that compares them. |
| Multisite with subdirectories, such as `example.com/site-a/` | One site report for the shared host. The subsites are not split. To look at one subsite, type its path, such as `/site-a/`, in a table's filter box. |

### Categories

Every request belongs to exactly one category. The script checks the rules in this order:

| Category | Rule | Top URLs in the site report |
| --- | --- | --- |
| Search | An `s` or `ep_filter_*` parameter, or a `/search/…` path | 500 search queries |
| Sitemap | `sitemap` in an `.xml` path | 200 |
| Asset | A JS, CSS, image, font, audio, video or PDF file, including `/_static/` | 50 |
| API | `/wp-json/`, `admin-ajax.php`, or a JSON response | 200 |
| Feed | An RSS or Atom response, or a `/feed` path | 200 |
| Page | Any other HTML response | 500 |
| Other | Everything else, such as `robots.txt` | 50 |

`ep_filter_*` parameters come from ElasticPress faceted search.

How each category shows its URLs:

- **Pages, feeds, sitemaps, assets and other** show the path without the query string.
- **Search** shows only the search parameters, for example `s=wordpress`.
- **API** shows the parameter names without values, for example `/?rest_route=/wp/v2/posts`.

### Columns in URL tables

Each URL row shows requests, share of the site's traffic, human share, bot share, cache hit rate, average response time and bandwidth. Click a column header to sort. Type in the filter box to search the rows.

### Other sections

- **Slowest URLs** ranks URLs by total response time, which is requests multiplied by average time. It leaves out assets, because their time is mostly the visitor's download time.
- **404 and 5xx tables** list the URLs that returned those errors most often.
- **Referrer domains** keep only the domain of the referring page, never the full URL.

### Crawler analysis

This section helps you find bad crawlers. It joins the ASN, user agent, proxy type and WPVIP traffic class, so one row shows who is sending traffic and what they request.

| Table | What a row is |
| --- | --- |
| Suspect Clients | A top client that matches at least one suspect rule. The Reason column says which rules matched. |
| Top Clients | One combination of ASN, proxy type, traffic class and user agent. This table holds the top 500. |
| ASNs | One network, with names for common ones such as AWS, Alibaba Cloud and Comcast. |
| User Agents | One user agent string. |
| TLS Fingerprints, JA4 | One TLS fingerprint, with the most distinct user agents seen with it in one day. |

**Column meanings:**
- **Datacenter IP** is the share of requests whose proxy type is `DCH`. The proxy type comes from WPVIP's `http_x_ip_proxy_type` field, and `DCH` appears on hosting networks such as AWS and Alibaba Cloud.
- **Search** and **Pages** are the share of requests in those categories. Bad crawlers often request only pages or search, with few assets.

**Suspect rules.** A client is flagged when it matches any of these rules:
- **Unclassified traffic from datacenter IPs.** The CDNs that carry iCloud Private Relay are left out, because their traffic is mostly real readers. Those CDNs are Cloudflare, Fastly and Akamai.
- **Unverified crawler user agent.** The user agent names a well-known crawler, such as Googlebot or GPTBot, but WPVIP classes it as People or leaves it unclassified. This often means a spoofed user agent.
- **Outdated browser.** The Chrome or Firefox version is 24 or more versions behind the newest one seen, which is about two years old.
- **Mostly search requests.** At least 50% of the client's requests are searches.

Suspects are leads to check, not proof. Review a client before you block it.

### Day folders and dates

S3 stores log files by the time WPVIP wrote them, not by request time. The first files in a day folder can hold requests from the end of the previous day. The reports chart each request on its real date. At a month boundary, a few minutes of requests count in the month whose folder holds them.

## Privacy

The reports do not keep:

- **IP addresses.** ASNs, proxy types and TLS fingerprints already identify bot networks.
- **Logged-in usernames.** The reports count logged-in requests only.
- **Full referrer URLs.** Only the referring domain is kept.

Search terms appear as visitors typed them, lowercased. Some may contain personal names. Share the reports only with people who may see that data.

## Check log size on S3

These commands show how much data a download will be before you run it.

```sh
# All edge logs
aws s3 ls --profile "$WPVIP_AWS_PROFILE" "$WPVIP_LOG_BUCKET/http_requests_edge/" --recursive --human-readable --summarize | tail -2

# One month
aws s3 ls --profile "$WPVIP_AWS_PROFILE" "$WPVIP_LOG_BUCKET/http_requests_edge/2026/08/" --recursive --human-readable --summarize | tail -2

# One day
aws s3 ls --profile "$WPVIP_AWS_PROFILE" "$WPVIP_LOG_BUCKET/http_requests_edge/2026/08/17/" --recursive --human-readable --summarize | tail -2
```

The script reads only edge logs. WPVIP also ships origin logs (`http_requests_origin`), but the edge logs already include every request that reached the origin.
