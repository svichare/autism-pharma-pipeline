"""
Configuration for the Autism Pharmacology Research Pipeline.

Environment variables:
    OPENAI_API_KEY       - OpenAI API key for LLM analysis
    MONGODB_URI          - MongoDB connection string (e.g. mongodb+srv://user:pass@cluster.mongodb.net/)
    MONGODB_DATABASE     - Database name (default: perp_autism_research)
    PUBMED_EMAIL         - Email for PubMed API (required by NCBI)
    PUBMED_MAX_RESULTS   - Max papers to fetch per query (default: 50)
    OPENAI_MODEL         - OpenAI model to use (default: gpt-4o-mini)
    PIPELINE_BATCH_SIZE  - How many papers to process per run (default: 20)
"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class Config:
    # OpenAI
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"

    # MongoDB
    mongodb_uri: str = ""
    mongodb_database: str = "perp_autism_research"

    # Collection names (all prefixed with perp_)
    col_categories: str = "perp_categories"
    col_sub_mechanisms: str = "perp_sub_mechanisms"
    col_drugs: str = "perp_drugs"
    col_papers: str = "perp_papers"
    col_pipeline_state: str = "perp_pipeline_state"

    # PubMed
    pubmed_email: str = ""
    pubmed_max_results: int = 50
    pubmed_rate_limit_delay: float = 0.4  # seconds between requests (NCBI asks for <=3/sec)

    # Pipeline
    batch_size: int = 20

    # Search queries for PubMed - covers the pharmacological landscape
    search_queries: List[str] = field(default_factory=lambda: [
        # Broad pharmacotherapy queries
        '"autism spectrum disorder" AND (pharmacotherapy OR "drug therapy" OR "clinical trial")',
        '"autism" AND ("randomized controlled trial"[pt]) AND (drug OR medication OR treatment)',
        
        # GABAergic
        '"autism" AND (GABA OR GABAergic OR bumetanide OR arbaclofen OR acamprosate OR ganaxolone)',
        
        # Glutamatergic
        '"autism" AND (memantine OR "d-cycloserine" OR riluzole OR "n-acetylcysteine" OR ketamine) AND trial',
        
        # Serotonergic
        '"autism" AND (SSRI OR fluoxetine OR citalopram OR buspirone OR sertraline) AND (trial OR treatment)',
        '"autism" AND (psilocybin OR MDMA OR psychedelic)',
        
        # Oxytocinergic
        '"autism" AND (oxytocin OR balovaptan OR vasopressin) AND (trial OR treatment)',
        
        # Dopaminergic / Antipsychotic
        '"autism" AND (risperidone OR aripiprazole OR lurasidone) AND (trial OR efficacy)',
        
        # Cholinergic
        '"autism" AND (donepezil OR galantamine OR nicotinic) AND treatment',
        
        # ADHD medications in ASD
        '"autism" AND (methylphenidate OR atomoxetine OR guanfacine) AND (ADHD OR hyperactivity)',
        
        # mTOR
        '"autism" AND (rapamycin OR everolimus OR mTOR) AND (tuberous sclerosis OR treatment)',
        
        # Neuroinflammation
        '"autism" AND (sulforaphane OR minocycline OR celecoxib OR "palmitoylethanolamide")',
        
        # Cannabinoid
        '"autism" AND (cannabidiol OR CBD OR cannabis) AND (trial OR treatment)',
        
        # Gut-brain
        '"autism" AND (probiotic OR "fecal transplant" OR "microbiota transfer") AND treatment',
        
        # Metabolic / Hormonal
        '"autism" AND (melatonin OR "folinic acid" OR leucovorin OR "vitamin D" OR methylcobalamin) AND trial',
        
        # Emerging
        '"autism" AND (suramin OR trofinetide OR "IGF-1" OR mecasermin)',
    ])

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            openai_api_key=os.environ.get("OPENAI_API_KEY", ""),
            openai_model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
            mongodb_uri=os.environ.get("MONGODB_URI", ""),
            mongodb_database=os.environ.get("MONGODB_DATABASE", "perp_autism_research"),
            pubmed_email=os.environ.get("PUBMED_EMAIL", ""),
            pubmed_max_results=int(os.environ.get("PUBMED_MAX_RESULTS", "50")),
            batch_size=int(os.environ.get("PIPELINE_BATCH_SIZE", "20")),
        )

    def validate(self):
        """Validate that required config values are set."""
        errors = []
        if not self.openai_api_key:
            errors.append("OPENAI_API_KEY is required")
        if not self.mongodb_uri:
            errors.append("MONGODB_URI is required")
        if not self.pubmed_email:
            errors.append("PUBMED_EMAIL is required (NCBI requires an email for API access)")
        if errors:
            raise ValueError("Configuration errors:\n  - " + "\n  - ".join(errors))
