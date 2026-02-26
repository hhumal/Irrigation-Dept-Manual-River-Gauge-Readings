# 🌧️ Sri Lanka Irrigation Department Rainfall Data Scraper

Automated rainfall data scraper for the **Sri Lanka Irrigation Department ArcGIS Database**.

This project connects to the official ArcGIS REST API, downloads all available rainfall gauge records, cleans the dataset, converts timestamps to Sri Lanka Time (UTC+5:30), and automatically maintains **individual CSV files per station**.

Designed to run automatically via **GitHub Actions**.

---

## 🚀 Features

* ✅ Connects directly to ArcGIS REST FeatureServer
* ✅ Automatically paginates beyond API transfer limits
* ✅ Cleans system fields
* ✅ Converts timestamps to Sri Lanka Time (UTC+5:30)
* ✅ Groups data by individual rainfall gauge
* ✅ Appends new data without duplicating historical records
* ✅ Automatically updates station CSV files
* ✅ GitHub Actions compatible

---

## 📡 Data Source

ArcGIS FeatureServer endpoint:

```
https://services3.arcgis.com/J7ZFXmR8rSmQ3FGf/arcgis/rest/services/gauges_2_view/FeatureServer/0/query
```

The script pulls all available records using:

* `where=1=1` (no filtering)
* `outFields=*` (all attributes)
* Ordered pagination using `OBJECTID ASC`

---

## 📂 Output Structure

After execution, the script creates:

```
Station_Rainfall_CSVs/
    ├── Station_A.csv
    ├── Station_B.csv
    ├── Station_C.csv
    └── ...
```

Each CSV file:

* Contains rainfall data for one station
* Is chronologically sorted (oldest → newest)
* Has duplicates removed
* Updates automatically on each run

---

## 🕒 Timezone Handling

All timestamp fields containing:

* `"time"`
* `"date"`

Are automatically:

1. Converted from Unix milliseconds
2. Adjusted to **UTC +5:30 (Sri Lanka Time)**
3. Exported without timezone metadata for CSV compatibility

---

## 🧠 Data Cleaning

The script automatically:

* Renames `CreationDate` → `Observation_Time`
* Removes unnecessary system columns:

  * `globalid`
  * `Creator`
  * `EditDate`
  * `Editor`
* Drops duplicate records based on `Observation_Time`

---

## ⚙️ Requirements

Install dependencies:

```bash
pip install requests pandas
```

Python version:

```
Python 3.8+
```

---

## ▶️ Running Locally

```bash
python scraper.py
```

After running, updated station CSVs will appear in:

```
Station_Rainfall_CSVs/
```

---

## 🤖 GitHub Actions Automation (Example)

Example workflow file:

```yaml
name: Update Rainfall Data

on:
  schedule:
    - cron: '0 */6 * * *'  # Runs every 6 hours
  workflow_dispatch:

jobs:
  update-data:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout Repository
      uses: actions/checkout@v3

    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.10'

    - name: Install Dependencies
      run: pip install requests pandas

    - name: Run Scraper
      run: python scraper.py

    - name: Commit & Push Changes
      run: |
        git config --global user.name 'github-actions'
        git config --global user.email 'actions@github.com'
        git add .
        git commit -m "Auto-update rainfall data" || echo "No changes to commit"
        git push
```

---

## 🔄 How Incremental Updates Work

For each station:

1. Existing CSV is loaded (if present)
2. New API data is appended
3. Duplicate timestamps are removed
4. Data is sorted chronologically
5. File is saved back cleanly

This ensures:

* No data loss
* No duplicate rows
* Seamless historical continuity

---

## 📊 Example Data Columns

Typical dataset fields may include:

* `gauge`
* `basin`
* `rainfall`
* `Observation_Time`
* Other ArcGIS attribute fields

(Exact columns depend on the live API schema.)

---

## 🛡️ Error Handling

The script safely handles:

* HTTP errors
* ArcGIS API errors
* Pagination limits
* Missing columns
* Empty datasets

---

## 📈 Future Improvements (Optional Ideas)

* Add rainfall visualization dashboard
* Upload processed data to cloud storage
* Add anomaly detection
* Add rainfall summary statistics per basin
* Convert to Parquet format for performance

---

## 📜 License

MIT License

---

## 👨‍💻 Author

Maintained for automated environmental data collection and open data research.

---

* A **professional data-science style README**
* Or a **badge-enhanced GitHub README** with shields (build status, last update, etc.)
