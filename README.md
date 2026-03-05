# Defense Signals™ — The Opportunity Floor
### Daily Federal Opportunity Intelligence Pipeline

Pulls SAM.gov opportunities every weekday at 6:00 AM CT, scores them,
and generates ready-to-send email, LinkedIn, and article content.

---

## What This Produces (Daily)

| File | Purpose |
|------|---------|
| `output/opportunity_floor.json` | Live feed for Kajabi widget |
| `output/daily_email.html` | Paste into ActiveCampaign |
| `output/daily_email.txt` | Plain text fallback |
| `output/linkedin_post.txt` | Copy → paste to LinkedIn |
| `output/daily_article.md` | Signal Room™ member post |

---

## Setup (One Time — ~10 minutes)

### Step 1 — Get your SAM.gov API key
1. Go to [SAM.gov](https://sam.gov) → Sign in → Account Details
2. Under **API Keys**, generate a key
3. Copy it — you'll need it in Step 3

### Step 2 — Create this repo on GitHub
1. Go to GitHub.com → **New Repository**
2. Name it: `opportunity-floor` (private is fine)
3. Push these files to it

### Step 3 — Add your SAM API key as a GitHub Secret
1. In your repo → **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret**
3. Name: `SAM_API_KEY`
4. Value: paste your SAM.gov API key
5. Click **Add secret**

### Step 4 — Enable GitHub Actions
1. In your repo → click the **Actions** tab
2. Click **Enable Actions** if prompted

### Step 5 — Run it manually the first time
1. Actions tab → **Opportunity Floor — Daily Pipeline**
2. Click **Run workflow** → **Run workflow**
3. Watch it run (takes ~2 minutes)
4. Check the `output/` folder — files will appear

---

## Kajabi Widget Setup

After your first successful run:

1. Go to your repo → `output/opportunity_floor.json`
2. Click **Raw** → copy that URL
3. It will look like:
   `https://raw.githubusercontent.com/YOUR_USERNAME/opportunity-floor/main/output/opportunity_floor.json`
4. Open `kajabi_widget.html` (in the pipeline output)
5. Replace `YOUR_CDN_OR_HOST` with that raw GitHub URL
6. In Kajabi → Signal Room™ post → add a **Custom Code** block
7. Paste the entire widget HTML

---

## Schedule

Runs automatically **Monday–Friday at 6:00 AM CT**.

To also run on weekends, edit `.github/workflows/opportunity_floor.yml`:
```yaml
- cron: '0 11 * * *'   # Every day including weekends
```

To run manually anytime: Actions → Opportunity Floor → Run workflow

---

## Scoring Logic

| Signal | Points |
|--------|--------|
| Agency match (Army/Navy/DLA/USAF/DHS) | +30 |
| NAICS match (541330/541614/541512/541519/541690) | +25 |
| Keyword in title | +15 |
| Preferred set-aside (WOSB/8a/SB) | +10 |
| Response deadline 7–21 days out | +5 |
| Blocklist hit (construction/janitorial/etc.) | −50 |

---

## Troubleshooting

**Zero results?** SAM.gov API sometimes has narrow windows. Try setting
`WINDOW_HOURS` to `48` in the workflow env vars.

**401 error?** Your SAM_API_KEY secret is wrong or expired. Regenerate at SAM.gov.

**Rate limited?** The script retries automatically with backoff. If persistent,
add `WINDOW_HOURS: '48'` and reduce run frequency to every other day.

---

*Defense Signals™ · LogiCore Corporation · Huntsville, Alabama*
