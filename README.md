# Autism Pharmacology Research Pipeline

An automated pipeline that continuously discovers, analyzes, and catalogs peer-reviewed research on pharmacological interventions for Autism Spectrum Disorder (ASD).

The pipeline:
1. **Fetches** new papers from PubMed using 16 targeted search queries covering all major pharmacological mechanisms
2. **Analyzes** each paper using OpenAI to extract structured data (drug name, mechanism of action, results, etc.)
3. **Stores** everything in MongoDB with a hierarchical structure: Categories → Sub-mechanisms → Drugs → Papers

## What's in the Database

The pipeline populates 5 MongoDB collections (all prefixed with `perp_`):

| Collection | Description |
|---|---|
| `perp_papers` | Individual research papers with full extracted data |
| `perp_drugs` | Unique drugs, linked to sub-mechanisms and categories |
| `perp_sub_mechanisms` | Pharmacological sub-mechanisms (e.g., "NMDA Receptor Antagonism") |
| `perp_categories` | 13 top-level mechanism categories |
| `perp_pipeline_state` | Pipeline run history and tracking |

### Mechanism Categories Covered

| Category | Examples |
|---|---|
| GABAergic System | Bumetanide, Arbaclofen, Acamprosate |
| Glutamatergic System | Memantine, NAC, D-Cycloserine, Ketamine |
| Serotonergic System | Fluoxetine, Buspirone, Psilocybin, MDMA |
| Oxytocinergic System | Intranasal Oxytocin, Balovaptan |
| Dopaminergic / Antipsychotic | Risperidone, Aripiprazole |
| Cholinergic System | Galantamine, Donepezil |
| Adrenergic / Stimulant | Methylphenidate, Atomoxetine, Guanfacine |
| mTOR Pathway | Everolimus, Rapamycin |
| Neuroinflammation / Immune | Sulforaphane, Celecoxib, PEA |
| Cannabinoid System | CBD, Cannabis extracts |
| Gut-Brain Axis | Probiotics, Microbiota Transfer Therapy |
| Hormonal / Metabolic | Melatonin, Leucovorin, Vitamin D |
| Other Emerging Targets | Suramin, IGF-1, Trofinetide |

## Prerequisites

- Python 3.9+
- MongoDB Atlas cluster (or any MongoDB 5.0+ instance)
- OpenAI API key
- Email address (required by NCBI for PubMed API access)

## Setup

### 1. Clone and install

```bash
git clone https://github.com/<your-username>/autism-pharma-pipeline.git
cd autism-pharma-pipeline
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
OPENAI_API_KEY=sk-your-key-here
MONGODB_URI=mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority
MONGODB_DATABASE=perp_autism_research
PUBMED_EMAIL=your-email@example.com
```

### 3. First-time setup (seed + run)

```bash
# Load the 140 pre-analyzed papers into MongoDB
python run.py seed

# Or do everything at once: seed + fetch new + rebuild
python run.py full
```

## Usage

### Commands

```bash
# Fetch new papers, analyze, and store
python run.py run

# Load seed data (140 pre-analyzed papers)
python run.py seed

# First-time setup: seed + run
python run.py full

# Rebuild aggregate collections from papers
python run.py rebuild

# Show database statistics
python run.py stats
```

### Running as a module

```bash
python -m src run
python -m src seed
python -m src stats
```

### Periodic Runs

To keep the knowledge base updated, schedule the pipeline to run periodically:

```bash
# Cron job (daily at 3 AM)
0 3 * * * cd /path/to/autism-pharma-pipeline && /path/to/venv/bin/python run.py run >> /var/log/pharma-pipeline.log 2>&1
```

Each run is incremental — it only fetches papers published since the last run and skips papers already in the database.

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   PubMed API    │────▶│   LLM Analyzer   │────▶│    MongoDB      │
│  (E-utilities)  │     │   (OpenAI)       │     │                 │
│                 │     │                  │     │  perp_papers    │
│  16 queries     │     │  Extracts:       │     │  perp_drugs     │
│  covering all   │     │  - Drug name     │     │  perp_sub_mech  │
│  mechanisms     │     │  - Mechanism     │     │  perp_categories│
│                 │     │  - Results       │     │  perp_state     │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

### How it works

1. **Fetch**: The PubMed fetcher runs 16 search queries covering GABAergic, glutamatergic, serotonergic, oxytocinergic, dopaminergic, cholinergic, adrenergic, mTOR, neuroinflammation, cannabinoid, gut-brain, metabolic, and emerging targets.

2. **Deduplicate**: PMIDs already in the database are skipped. On subsequent runs, only papers published after the last run are fetched.

3. **Analyze**: Each paper's title and abstract are sent to OpenAI (gpt-4o-mini by default). The LLM extracts the drug name, mechanism of action, mechanism category, sub-mechanism, study type, sample size, target symptoms, results, and trial phase.

4. **Store**: Papers are upserted into `perp_papers` using deterministic IDs (content-hashed). This makes the operation idempotent.

5. **Aggregate**: After storing papers, the pipeline rebuilds `perp_drugs`, `perp_sub_mechanisms`, and `perp_categories` by aggregating from the papers collection. Each drug, sub-mechanism, and category gets an accurate paper count and cross-references.

### MongoDB Schema

**perp_papers**
```json
{
  "paper_id": "a1b2c3d4e5f6",
  "pmid": "12345678",
  "title": "...",
  "authors": "Smith J, Doe A et al.",
  "journal": "Journal of Autism",
  "year": 2024,
  "url": "https://pubmed.ncbi.nlm.nih.gov/12345678/",
  "abstract_summary": "...",
  "drug_name": "Bumetanide",
  "drug_id": "f6e5d4c3b2a1",
  "mechanism_of_action": "NKCC1 chloride transporter antagonist...",
  "category_id": "...",
  "category_name": "GABAergic System",
  "sub_mechanism_id": "...",
  "sub_mechanism_name": "NKCC1 Chloride Transporter Antagonism",
  "study_type": "Randomized Controlled Trial",
  "sample_size": "88",
  "target_symptoms": "Core social communication deficits",
  "results_summary": "Significant improvement in...",
  "result_direction": "positive",
  "trial_phase": "Phase 2"
}
```

**perp_categories**
```json
{
  "category_id": "...",
  "name": "GABAergic System",
  "icon": "🔵",
  "description": "Drugs targeting gamma-aminobutyric acid (GABA) signaling...",
  "paper_count": 15,
  "sub_mechanism_count": 5,
  "sub_mechanism_names": ["GABA-B Receptor Agonism", "NKCC1 Chloride Transporter Antagonism", ...]
}
```

## Configuration

| Variable | Required | Default | Description |
|---|---|---|---|
| `OPENAI_API_KEY` | Yes (for `run`) | — | OpenAI API key |
| `MONGODB_URI` | Yes | — | MongoDB connection string |
| `MONGODB_DATABASE` | No | `perp_autism_research` | Database name |
| `PUBMED_EMAIL` | Yes (for `run`) | — | Email for NCBI API access |
| `OPENAI_MODEL` | No | `gpt-4o-mini` | Model for analysis |
| `PUBMED_MAX_RESULTS` | No | `50` | Max papers per query |
| `PIPELINE_BATCH_SIZE` | No | `20` | Papers to process per run |

## Cost Estimate

- **PubMed**: Free (public API, rate-limited to 3 req/sec)
- **OpenAI (gpt-4o-mini)**: ~$0.01-0.02 per paper analyzed (~$0.20-0.40 per run of 20 papers)
- **OpenAI (gpt-4o)**: ~$0.05-0.10 per paper (~$1-2 per run of 20 papers)
- **MongoDB Atlas**: Free tier (M0) supports up to 512MB, sufficient for thousands of papers

## License

MIT
