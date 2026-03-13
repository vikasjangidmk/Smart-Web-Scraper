# 🎯 Smart Lead Scraper — IndiaMART Zero-Hallucination Pipeline

A highly optimized, highly specific B2B web scraping pipeline designed to extract high-quality, verified company records from **IndiaMART**. It utilizes Playwright, Regex, and a dual-layer LLM architecture to absolutely guarantee zero hallucination and mathematically verifiable records, specifically optimized for multi-city regional targeting (e.g. Gujarat).

## 🚀 Key Features

* **Multi-City & Multi-Keyword Support**: Searches IndiaMART intelligently by splitting keywords and cities into individual runs to bypass IndiaMART's `cq` (city) limitations, maximizing lead volume.
* **Root Profile Normalization**: Automatically strips away testimonial, catalogue, or product sub-pages and resolves down to the true company `ROOT` profile, avoiding LLM confusion and duplicate processing.
* **Intelligent Geo-Filtering**: Built-in state validation (e.g., Gujarat state filter). Intelligently determines geography by analyzing explicit state mentions, falling back to known city lists, and even inspecting the prefix of the GST code (e.g. `24` = Gujarat).
* **Zero Hallucination Architecture**:
  * **Stage 0 (Python)**: Uses Regex to pull definitive, immutable facts (GST numbers, Phone numbers) directly from the raw HTML.
  * **Stage 1 (LLM)**: Extracts unstructured text directly from the HTML source.
  * **Stage 2 (Python)**: Reconciles the LLM extractions against the regex-proven facts, overwriting any hallucinations.
  * **Stage 3 (LLM)**: Formats the strictly validated objects into the final nested JSON schema.
* **Incremental Checkpointing**: Actively skips duplicate URLs and uses a checkpointing system to save its output row-by-row so data is never lost.

## 🛠️ Architecture and Stack

- **Crawler Layer**: `playwright` with stealth overrides to bypass basic detection metrics.
- **HTML Processing**: `beautifulsoup4`
- **Validation Engine**: Custom Python regex and logic rules (`validator/data_cleaner.py`)
- **LLM Engine**: `OpenRouter` (`deepseek/deepseek-chat-v3-0324`) using specific extraction prompts.

## 📂 Project Structure

```text
Smart Web Scraper/
├── config/
│   └── settings.yaml      # All core configurations (keywords, cities, crawler timings)
├── crawler/
│   └── indiamart.py       # Specially tuned IndiaMART crawler with multi-query logic
├── Output/
│   └── leads.json         # Final highly-structured, verified output
├── parser/
│   └── llm_extractor.py   # Code invoking OpenRouter APIs
├── storage/
│   └── save_json.py       # Checkpointing and JSON output utilities
├── utils/
│   ├── logger.py
│   └── retry.py
├── validator/
│   ├── data_cleaner.py    # The core validator and intelligent state/GST filter
│   ├── phone_validator.py
│   └── gst_validator.py
├── main.py                # Pipeline orchestrator
├── README.md              # You are here
└── requirements.txt
```

## ⚙️ Configuration
All configurations such as target cities, target search queries, output paths, timeouts, and LLM controls are available in `config/settings.yaml`. 

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
- Average completeness score
- Confidence tier breakdown
- Missing data statistics (Companies without GST, emails, etc)
- Complete drop-reason analytics (e.g., how many records dropped due to `not_gujarat_state` or `no_contact_info`)
