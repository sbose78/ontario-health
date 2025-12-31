# Hospital Network Scrapers

Framework for scraping ED wait times from Ontario hospital networks.

## Current Production

**Working**: 3 Halton Healthcare hospitals (Georgetown, Milton, Oakville)

**Scraper**: `ingest_ed_wait_times.py` (original Halton scraper)

## Expansion Framework

**Created**: Multi-network infrastructure
- `base.py` - Abstract base class
- Individual network scrapers (stubs)
- Multi-network coordinator: `ingest_all_ed_wait_times.py`

## Why Only 3 Hospitals?

After testing 11+ hospital networks, most Ontario hospitals:

❌ **JavaScript-rendered** - Wait times loaded dynamically (Niagara, Lakeridge, St Mary's)  
❌ **No live data** - Only general info, no actual times (UHN Toronto)  
❌ **Embedded dashboards** - Power BI/Tableau (Southlake, Peterborough)  
❌ **API protected** - JSON endpoints require auth (Hamilton)  
❌ **Blocked/403** - Anti-scraping measures (Windsor)  

✅ **Static HTML with data** - Only Halton Healthcare confirmed working

## To Expand (Future)

### Option 1: Selenium/Playwright
```python
# For JavaScript sites
from selenium import webdriver
driver.get(url)
time.sleep(2)  # Wait for JavaScript
data = driver.page_source
```

**Tradeoff**: Slower, more complex, requires browser runtime

### Option 2: API Reverse Engineering
Some sites (Hamilton) have JSON endpoints. Requires:
- Network inspection in browser DevTools
- Finding internal API URLs
- May violate TOS

### Option 3: Official APIs
Advocate for Ontario to provide unified ED wait time API.

## Hospitals Researched

From v2 research document:
- Niagara Health (4 hospitals) - JS rendered
- Hamilton (3-4 hospitals) - JSON API (complex)
- Lakeridge (4 hospitals) - No visible data
- UHN Toronto (2 hospitals) - No live times
- London Health (2 hospitals) - Policy text only
- Windsor Regional (2 hospitals) - 403 blocked
- Southlake (1 hospital) - Power BI embedded
- Montfort (1 hospital) - Mixed results
- St Mary's (1 hospital) - No data found
- Peterborough (1 hospital) - Dashboard embed
- Oculys Network (3 hospitals) - Needs investigation

**Total researched: 25-29 hospitals**  
**Actually scrapeable with current approach: 3 hospitals**

## Conclusion

Halton Healthcare is the most reliable data source.
Multi-network framework is ready, but each site needs custom implementation.
Recommend focusing on data quality from 3 hospitals vs unreliable data from 20+.

