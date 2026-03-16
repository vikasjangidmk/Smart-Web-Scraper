# 🎯 Smart Lead Scraper — Multi-Source Zero-Hallucination Pipeline

A highly optimized, multi-source B2B web scraping pipeline designed to extract high-quality, verified company records from **IndiaMART, TradeIndia, and ExportersIndia**. It utilizes Playwright, Regex, and a dual-layer LLM architecture to absolutely guarantee zero hallucination and mathematically verifiable records.

## 🚀 Key Features

* **Concurrent Multi-Source Crawling**: Uses `ThreadPoolExecutor` to run Playwright crawlers for IndiaMART, TradeIndia, and ExportersIndia in true parallel, maximizing data acquisition speed.
* **Pre-LLM Strict Gating**: Employs deep Python regex validation on raw HTML to instantly drop candidates missing mandatory criteria (like Phone Numbers or GST Codes) *before* expensive LLM API calls, saving time and money.
* **Intelligent Geo-Filtering**: Built-in state validation (e.g., Gujarat state filter). Intelligently determines geography by analyzing explicit state mentions, falling back to known city lists, and inspecting the prefix of the GST code (e.g. `24` = Gujarat).
* **Zero Hallucination Architecture**:
  * **Stage 0 (Python)**: Uses Regex to pull definitive, immutable facts (GST numbers, Phone numbers) directly from the raw HTML.
  * **Stage 1 (LLM)**: Extracts unstructured text directly from the HTML source.
  * **Stage 2 (Python)**: Reconciles the LLM extractions against the regex-proven facts, overwriting any hallucinations.
  * **Stage 3 (LLM)**: Formats the strictly validated objects into the final nested JSON schema.
* **Incremental Deduplication & Deduplication**: Actively skips duplicate URLs across all sources globally before they reach the LLM, and explicitly cross-references exact phone numbers and names to prevent duplicates in the final output.

## 🛠️ Architecture and Stack

- **Crawler Layer**: `playwright` with concurrency wrappers handling three sources.
- **HTML Processing**: `beautifulsoup4`
- **Validation Engine**: Custom Python regex and logic rules (`validator/data_cleaner.py`)
- **LLM Engine**: `OpenRouter` (`deepseek/deepseek-chat-v3-0324`) using multi-stage strict validation.

## 📂 Project Structure

```text
Smart Web Scraper/
├── config/
│   └── settings.yaml      # All core configurations (keywords, cities, crawler timings, parallel threads)
├── crawler/
│   ├── indiamart.py       # IndiaMART crawler logic
│   ├── tradeindia.py      # TradeIndia crawler logic 
│   └── exportersindia.py  # ExportersIndia crawler logic
├── Output/
│   └── leads.json         # Final highly-structured, verified output
├── parser/
│   └── llm_extractor.py   # Code invoking OpenRouter APIs securely
├── storage/
│   └── save_json.py       # JSON output format utilities
├── utils/
│   ├── logger.py
│   └── retry.py
├── validator/
│   ├── data_cleaner.py    # The core validator and intelligent state/GST filter
│   ├── phone_validator.py
│   └── gst_validator.py
├── main.py                # Pipeline multi-threaded orchestrator
├── README.md              # You are here
└── requirements.txt
```

## ⚙️ Configuration
All configurations such as target cities, keyword combinations, output paths, and strict LLM controls are available in `config/settings.yaml`. 

For targeting different cities or products, adjust the `search_queries` section in `settings.yaml`:
```yaml
search_queries:
  - keyword: "bubble wrap"
    cities: ["Ahmedabad", "Rajkot", "Surat", "Vadodara"]
```

## 💻 How to Run

1. **Install requirements:**
```bash
pip install -r requirements.txt
playwright install chromium
```

2. **Supply your API keys:**
Ensure you have a `.env` file containing your OpenRouter API key:
```env
OPENROUTER_API_KEY=your_key_here
```

3. **Run the pipeline:**
```bash
python main.py
```

## 📊 Analytics & Reporting

Upon completion, `main.py` provides you with a beautiful pipeline summary showing exactly your coverage:
- Average completeness score %
- Confidence tier breakdown
- Missing data statistics (Companies without GST, emails, etc)
- Complete drop-reason analytics globally across all 3 platforms.
