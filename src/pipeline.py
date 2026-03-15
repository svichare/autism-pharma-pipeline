"""
Main Pipeline Orchestrator — ties together PubMed fetching, LLM analysis, and MongoDB storage.

Usage:
    python -m src.pipeline run          # Fetch new papers, analyze, store
    python -m src.pipeline seed         # Load seed data (existing 140 papers) into MongoDB
    python -m src.pipeline rebuild      # Rebuild aggregate collections from papers
    python -m src.pipeline stats        # Show database statistics
    python -m src.pipeline full         # seed + run + rebuild (first-time setup)
"""

import json
import sys
import logging
from pathlib import Path
from typing import List, Dict

from .config import Config
from .pubmed_fetcher import PubMedFetcher, PubMedPaper
from .llm_analyzer import LLMAnalyzer
from .mongo_store import MongoStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, config: Config):
        self.config = config
        self.store = MongoStore(
            uri=config.mongodb_uri,
            database=config.mongodb_database,
            col_categories=config.col_categories,
            col_sub_mechanisms=config.col_sub_mechanisms,
            col_drugs=config.col_drugs,
            col_papers=config.col_papers,
            col_pipeline_state=config.col_pipeline_state,
        )
        self.fetcher = PubMedFetcher(
            email=config.pubmed_email,
            rate_limit_delay=config.pubmed_rate_limit_delay,
        )
        self.analyzer = LLMAnalyzer(
            api_key=config.openai_api_key,
            model=config.openai_model,
        )

    def run(self):
        """
        Main pipeline: fetch new papers from PubMed, analyze with LLM, store in MongoDB.
        
        - Checks which PMIDs are already in the database to avoid re-processing
        - Uses the last run date to narrow PubMed searches to new papers
        - Processes papers in batches to control LLM API costs
        """
        logger.info("=" * 60)
        logger.info("PIPELINE RUN STARTED")
        logger.info("=" * 60)

        # Get already-seen PMIDs
        seen_pmids = self.store.get_seen_pmids()
        logger.info(f"Already have {len(seen_pmids)} papers in database")

        # Get last run date for incremental fetching
        last_run_date = self.store.get_last_run_date()
        if last_run_date:
            logger.info(f"Last pipeline run: {last_run_date} — fetching papers since then")
        else:
            logger.info("No previous run found — fetching recent papers (no date filter)")

        # Fetch papers from PubMed across all search queries
        new_papers: List[PubMedPaper] = []

        for i, query in enumerate(self.config.search_queries):
            logger.info(f"Query {i + 1}/{len(self.config.search_queries)}: {query[:80]}...")
            papers = self.fetcher.search_and_fetch(
                query=query,
                max_results=self.config.pubmed_max_results,
                min_date=last_run_date,
            )

            # Filter out already-seen papers
            for paper in papers:
                if paper.pmid not in seen_pmids:
                    new_papers.append(paper)
                    seen_pmids.add(paper.pmid)  # avoid duplicates across queries

        logger.info(f"Found {len(new_papers)} new papers to analyze")

        if not new_papers:
            logger.info("No new papers found. Pipeline run complete.")
            self.store.save_pipeline_run({
                "status": "completed",
                "new_papers_found": 0,
                "papers_stored": 0,
            })
            return

        # Limit to batch size
        batch = new_papers[:self.config.batch_size]
        logger.info(f"Processing batch of {len(batch)} papers (batch_size={self.config.batch_size})")

        # Convert to dicts for LLM analysis
        papers_for_analysis = []
        for p in batch:
            papers_for_analysis.append({
                "pmid": p.pmid,
                "title": p.title,
                "abstract": p.abstract,
                "authors": p.authors,
                "journal": p.journal,
                "year": p.year,
                "doi": p.doi,
                "url": p.url,
            })

        # Analyze with LLM
        analyzed = self.analyzer.analyze_batch(papers_for_analysis)
        logger.info(f"LLM analysis complete: {len(analyzed)} relevant papers extracted")

        if analyzed:
            # Remap fields for storage
            for paper in analyzed:
                paper["abstract_summary"] = paper.pop("abstract", "")
                paper["mechanism_category"] = paper.pop("mechanism_category", "Other Emerging Targets")
                paper["sub_mechanism"] = paper.pop("sub_mechanism", "Unclassified")

            # Store in MongoDB
            stored = self.store.upsert_papers(analyzed)
            logger.info(f"Stored {stored} papers in MongoDB")

            # Rebuild aggregates
            self.store.rebuild_aggregates()
        else:
            stored = 0

        # Record pipeline run
        self.store.save_pipeline_run({
            "status": "completed",
            "new_papers_found": len(new_papers),
            "papers_analyzed": len(batch),
            "papers_relevant": len(analyzed),
            "papers_stored": stored,
            "queries_run": len(self.config.search_queries),
        })

        logger.info("=" * 60)
        logger.info(f"PIPELINE RUN COMPLETE — {stored} new papers added")
        logger.info("=" * 60)

    def seed(self, seed_file: str = "seed_data/existing_papers.json"):
        """
        Load existing research data (the 140 papers from initial analysis) into MongoDB.
        This is idempotent — running it twice won't create duplicates.
        """
        logger.info("=" * 60)
        logger.info("SEEDING DATABASE WITH EXISTING PAPERS")
        logger.info("=" * 60)

        seed_path = Path(seed_file)
        if not seed_path.exists():
            # Try relative to project root
            seed_path = Path(__file__).parent.parent / seed_file
        if not seed_path.exists():
            logger.error(f"Seed file not found: {seed_file}")
            return

        with open(seed_path) as f:
            papers = json.load(f)

        logger.info(f"Loaded {len(papers)} papers from seed file")

        # Map fields from seed format to storage format
        mapped = []
        for p in papers:
            mapped.append({
                "pmid": p.get("pmid", ""),
                "title": p.get("title", ""),
                "authors": p.get("authors", ""),
                "journal": p.get("journal", ""),
                "year": p.get("year", 0),
                "url": p.get("url", ""),
                "doi": p.get("doi", ""),
                "abstract_summary": p.get("abstract_summary", ""),
                "drug_name": p.get("drug_name", ""),
                "mechanism_of_action": p.get("mechanism_of_action", ""),
                "mechanism_category": p.get("mechanism_category", ""),
                "sub_mechanism": p.get("clean_sub_mechanism", p.get("sub_mechanism", "")),
                "study_type": p.get("study_type", ""),
                "sample_size": str(p.get("sample_size", "N/A")),
                "target_symptoms": p.get("target_symptoms", ""),
                "results_summary": p.get("results_summary", ""),
                "result_direction": p.get("result_direction", "mixed"),
                "trial_phase": p.get("trial_phase", "N/A"),
            })

        stored = self.store.upsert_papers(mapped)
        self.store.rebuild_aggregates()

        logger.info(f"Seeded {stored} papers into database")
        stats = self.store.get_stats()
        logger.info(f"Database stats: {stats}")

    def rebuild(self):
        """Rebuild aggregate collections from the papers collection."""
        logger.info("Rebuilding aggregate collections...")
        self.store.rebuild_aggregates()
        stats = self.store.get_stats()
        logger.info(f"Rebuild complete. Stats: {stats}")

    def stats(self):
        """Print database statistics."""
        stats = self.store.get_stats()
        print("\n📊 Database Statistics")
        print("=" * 40)
        print(f"  Papers:          {stats['total_papers']}")
        print(f"  Drugs:           {stats['total_drugs']}")
        print(f"  Sub-mechanisms:  {stats['total_sub_mechanisms']}")
        print(f"  Categories:      {stats['total_categories']}")
        print(f"  Pipeline runs:   {stats['total_runs']}")
        print("=" * 40)

    def close(self):
        """Clean up resources."""
        self.store.close()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].lower()
    valid_commands = {"run", "seed", "stats", "rebuild", "full"}

    if command not in valid_commands:
        print(f"Unknown command: {command}")
        print(f"Valid commands: {', '.join(sorted(valid_commands))}")
        sys.exit(1)

    config = Config.from_env()

    # seed and stats don't need OpenAI key
    if command in ("run", "full"):
        config.validate()
    elif command in ("seed", "rebuild", "stats"):
        if not config.mongodb_uri:
            raise ValueError("MONGODB_URI is required")

    pipeline = Pipeline(config)

    try:
        if command == "seed":
            seed_file = sys.argv[2] if len(sys.argv) > 2 else "seed_data/existing_papers.json"
            pipeline.seed(seed_file)
        elif command == "run":
            pipeline.run()
        elif command == "rebuild":
            pipeline.rebuild()
        elif command == "stats":
            pipeline.stats()
        elif command == "full":
            pipeline.seed()
            pipeline.run()
            pipeline.rebuild()
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
