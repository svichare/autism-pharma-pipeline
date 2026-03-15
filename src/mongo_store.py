"""
MongoDB Storage — manages all database operations for the knowledge base.

Collections (all prefixed with perp_):
  - perp_papers          : Individual research papers with full extracted data
  - perp_drugs           : Unique drugs, each linked to a sub-mechanism and category
  - perp_sub_mechanisms  : Sub-mechanisms grouped under categories
  - perp_categories      : Top-level mechanism categories
  - perp_pipeline_state  : Tracks pipeline runs, last fetch dates, seen PMIDs

All documents use deterministic IDs based on content hashing to enable
idempotent upserts — running the pipeline twice with the same data is safe.
"""

import hashlib
import logging
from typing import List, Dict, Optional, Set
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

logger = logging.getLogger(__name__)

# Category metadata
CATEGORY_META = {
    "GABAergic System": {
        "icon": "🔵",
        "description": "Drugs targeting gamma-aminobutyric acid (GABA) signaling — the brain's primary inhibitory neurotransmitter. Research suggests E/I imbalance in ASD, with GABA often functioning aberrantly."
    },
    "Glutamatergic System": {
        "icon": "🔴",
        "description": "Drugs modulating glutamate, the brain's primary excitatory neurotransmitter. The excitatory/inhibitory (E/I) imbalance hypothesis is central to ASD neurobiology."
    },
    "Serotonergic System": {
        "icon": "🟣",
        "description": "Drugs targeting serotonin (5-HT) pathways. Hyperserotonemia is one of the most replicated biomarker findings in ASD. SSRIs and novel serotonergic agents have been studied for repetitive behaviors and anxiety."
    },
    "Oxytocinergic System": {
        "icon": "💗",
        "description": "Drugs targeting the oxytocin/vasopressin neuropeptide system, critical for social bonding and social cognition — core deficit areas in ASD."
    },
    "Dopaminergic / Antipsychotic": {
        "icon": "🟠",
        "description": "Antipsychotic drugs modulating dopamine and serotonin receptors. Risperidone and aripiprazole are the only two FDA-approved medications for ASD-associated irritability."
    },
    "Cholinergic System": {
        "icon": "🟢",
        "description": "Drugs modulating acetylcholine signaling. Cholinergic dysfunction has been observed in ASD, with interest in both muscarinic and nicotinic receptor targets."
    },
    "Adrenergic / Stimulant": {
        "icon": "⚡",
        "description": "Stimulants and non-stimulant ADHD medications for the ~30-80% of ASD individuals with co-occurring attention/hyperactivity symptoms."
    },
    "mTOR Pathway": {
        "icon": "🧬",
        "description": "Drugs targeting the mechanistic target of rapamycin (mTOR) signaling pathway. mTOR overactivation is implicated in syndromic forms of ASD, particularly tuberous sclerosis complex (TSC)."
    },
    "Neuroinflammation / Immune Modulation": {
        "icon": "🛡️",
        "description": "Drugs targeting neuroinflammatory pathways and immune dysregulation. Microglial activation and elevated cytokines are observed in many ASD individuals."
    },
    "Cannabinoid System": {
        "icon": "🌿",
        "description": "Drugs modulating the endocannabinoid system (ECS). The ECS regulates neurotransmitter release, neuroinflammation, and synaptic plasticity — all implicated in ASD."
    },
    "Gut-Brain Axis": {
        "icon": "🦠",
        "description": "Interventions targeting the gut microbiome and its communication with the brain. GI symptoms are highly prevalent in ASD and microbiome differences are well-documented."
    },
    "Hormonal / Metabolic": {
        "icon": "⚗️",
        "description": "Metabolic interventions targeting nutritional deficiencies, methylation pathways, and circadian disruption commonly observed in ASD."
    },
    "Other Emerging Targets": {
        "icon": "🔬",
        "description": "Novel and investigational approaches including antipurinergic therapy, IGF-1 signaling, and vasopressin modulation."
    },
}


def _make_id(*parts: str) -> str:
    """Create a deterministic 12-char hex ID from input strings."""
    combined = ":".join(str(p) for p in parts)
    return hashlib.md5(combined.encode()).hexdigest()[:12]


class MongoStore:
    def __init__(self, uri: str, database: str, col_categories: str = "perp_categories",
                 col_sub_mechanisms: str = "perp_sub_mechanisms",
                 col_drugs: str = "perp_drugs", col_papers: str = "perp_papers",
                 col_pipeline_state: str = "perp_pipeline_state"):
        self.client = MongoClient(uri)
        self.db = self.client[database]
        self.categories = self.db[col_categories]
        self.sub_mechanisms = self.db[col_sub_mechanisms]
        self.drugs = self.db[col_drugs]
        self.papers = self.db[col_papers]
        self.pipeline_state = self.db[col_pipeline_state]

        # Create indexes
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create indexes for efficient querying."""
        self.papers.create_index("pmid", unique=True, sparse=True)
        self.papers.create_index("paper_id", unique=True)
        self.papers.create_index("category_name")
        self.papers.create_index("sub_mechanism_name")
        self.papers.create_index("drug_name")
        self.papers.create_index("year")

        self.drugs.create_index("drug_id", unique=True)
        self.drugs.create_index("category_name")

        self.sub_mechanisms.create_index("sub_mechanism_id", unique=True)
        self.sub_mechanisms.create_index("category_name")

        self.categories.create_index("category_id", unique=True)

        logger.info("MongoDB indexes ensured")

    def get_seen_pmids(self) -> Set[str]:
        """Return set of all PMIDs already in the database."""
        pmids = set()
        for doc in self.papers.find({"pmid": {"$exists": True, "$ne": ""}}, {"pmid": 1}):
            pmids.add(doc["pmid"])
        return pmids

    def upsert_papers(self, papers: List[Dict]) -> int:
        """
        Upsert analyzed papers into perp_papers collection.
        
        Each paper should have been through LLM analysis and contain:
        title, authors, journal, year, url, abstract_summary/abstract,
        drug_name, mechanism_of_action, mechanism_category, sub_mechanism,
        study_type, sample_size, target_symptoms, results_summary, trial_phase
        
        Returns the number of papers upserted.
        """
        if not papers:
            return 0

        ops = []
        for p in papers:
            paper_id = _make_id("paper", p.get("title", "")[:60], str(p.get("year", "")))
            cat_id = _make_id(p.get("mechanism_category", ""))
            sub_id = _make_id(p.get("mechanism_category", ""), p.get("sub_mechanism", ""))
            drug_id = _make_id(p.get("mechanism_category", ""),
                               p.get("sub_mechanism", ""),
                               p.get("drug_name", ""))

            doc = {
                "paper_id": paper_id,
                "pmid": p.get("pmid", ""),
                "title": p.get("title", ""),
                "authors": p.get("authors", ""),
                "journal": p.get("journal", ""),
                "year": p.get("year", 0),
                "url": p.get("url", ""),
                "doi": p.get("doi", ""),
                "abstract_summary": p.get("abstract_summary", p.get("abstract", "")),
                "drug_name": p.get("drug_name", ""),
                "drug_id": drug_id,
                "mechanism_of_action": p.get("mechanism_of_action", ""),
                "category_id": cat_id,
                "category_name": p.get("mechanism_category", ""),
                "sub_mechanism_id": sub_id,
                "sub_mechanism_name": p.get("sub_mechanism", ""),
                "study_type": p.get("study_type", ""),
                "sample_size": str(p.get("sample_size", "N/A")),
                "target_symptoms": p.get("target_symptoms", ""),
                "results_summary": p.get("results_summary", ""),
                "result_direction": p.get("result_direction", "mixed"),
                "trial_phase": p.get("trial_phase", "N/A"),
                "updated_at": datetime.now(timezone.utc),
            }

            ops.append(UpdateOne(
                {"paper_id": paper_id},
                {"$set": doc, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
                upsert=True
            ))

        try:
            result = self.db[self.papers.name].bulk_write(ops, ordered=False)
            count = result.upserted_count + result.modified_count
            logger.info(f"Upserted {count} papers ({result.upserted_count} new, {result.modified_count} updated)")
            return count
        except BulkWriteError as e:
            logger.error(f"Bulk write error: {e.details}")
            return 0

    def rebuild_aggregates(self):
        """
        Rebuild the drugs, sub_mechanisms, and categories collections
        from the current papers collection.
        
        This is an idempotent operation that can be run after every pipeline batch.
        It aggregates papers to derive the hierarchy.
        """
        logger.info("Rebuilding aggregate collections from papers...")

        # --- Aggregate drugs ---
        drug_pipeline = [
            {"$group": {
                "_id": {
                    "category_name": "$category_name",
                    "sub_mechanism_name": "$sub_mechanism_name",
                    "drug_name": "$drug_name",
                },
                "drug_id": {"$first": "$drug_id"},
                "category_id": {"$first": "$category_id"},
                "sub_mechanism_id": {"$first": "$sub_mechanism_id"},
                "mechanism_of_action": {"$first": "$mechanism_of_action"},
                "paper_count": {"$sum": 1},
            }}
        ]
        drug_results = list(self.papers.aggregate(drug_pipeline))

        drug_ops = []
        for d in drug_results:
            doc = {
                "drug_id": d["drug_id"],
                "category_id": d["category_id"],
                "category_name": d["_id"]["category_name"],
                "sub_mechanism_id": d["sub_mechanism_id"],
                "sub_mechanism_name": d["_id"]["sub_mechanism_name"],
                "name": d["_id"]["drug_name"],
                "mechanism_of_action": d.get("mechanism_of_action", ""),
                "paper_count": d["paper_count"],
                "updated_at": datetime.now(timezone.utc),
            }
            drug_ops.append(UpdateOne(
                {"drug_id": d["drug_id"]},
                {"$set": doc, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
                upsert=True
            ))

        if drug_ops:
            self.drugs.bulk_write(drug_ops, ordered=False)
            logger.info(f"Rebuilt {len(drug_ops)} drug entries")

        # --- Aggregate sub-mechanisms ---
        sub_pipeline = [
            {"$group": {
                "_id": {
                    "category_name": "$category_name",
                    "sub_mechanism_name": "$sub_mechanism_name",
                },
                "sub_mechanism_id": {"$first": "$sub_mechanism_id"},
                "category_id": {"$first": "$category_id"},
                "paper_count": {"$sum": 1},
                "drug_names": {"$addToSet": "$drug_name"},
            }}
        ]
        sub_results = list(self.papers.aggregate(sub_pipeline))

        sub_ops = []
        for s in sub_results:
            drug_names = sorted(s.get("drug_names", []))
            doc = {
                "sub_mechanism_id": s["sub_mechanism_id"],
                "category_id": s["category_id"],
                "category_name": s["_id"]["category_name"],
                "name": s["_id"]["sub_mechanism_name"],
                "paper_count": s["paper_count"],
                "drug_count": len(drug_names),
                "drug_names": drug_names,
                "updated_at": datetime.now(timezone.utc),
            }
            sub_ops.append(UpdateOne(
                {"sub_mechanism_id": s["sub_mechanism_id"]},
                {"$set": doc, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
                upsert=True
            ))

        if sub_ops:
            self.sub_mechanisms.bulk_write(sub_ops, ordered=False)
            logger.info(f"Rebuilt {len(sub_ops)} sub-mechanism entries")

        # --- Aggregate categories ---
        cat_pipeline = [
            {"$group": {
                "_id": "$category_name",
                "category_id": {"$first": "$category_id"},
                "paper_count": {"$sum": 1},
                "sub_mechanism_names": {"$addToSet": "$sub_mechanism_name"},
            }}
        ]
        cat_results = list(self.papers.aggregate(cat_pipeline))

        cat_ops = []
        for c in cat_results:
            cat_name = c["_id"]
            meta = CATEGORY_META.get(cat_name, {"icon": "📄", "description": ""})
            sub_names = sorted(c.get("sub_mechanism_names", []))
            doc = {
                "category_id": c["category_id"],
                "name": cat_name,
                "icon": meta["icon"],
                "description": meta["description"],
                "paper_count": c["paper_count"],
                "sub_mechanism_count": len(sub_names),
                "sub_mechanism_names": sub_names,
                "updated_at": datetime.now(timezone.utc),
            }
            cat_ops.append(UpdateOne(
                {"category_id": c["category_id"]},
                {"$set": doc, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
                upsert=True
            ))

        if cat_ops:
            self.categories.bulk_write(cat_ops, ordered=False)
            logger.info(f"Rebuilt {len(cat_ops)} category entries")

        logger.info("Aggregate rebuild complete")

    def save_pipeline_run(self, run_data: Dict):
        """Save a pipeline run record for tracking."""
        run_data["timestamp"] = datetime.now(timezone.utc)
        self.pipeline_state.insert_one(run_data)
        logger.info("Pipeline run state saved")

    def get_last_run_date(self) -> Optional[str]:
        """Get the date of the last successful pipeline run (YYYY/MM/DD format for PubMed)."""
        last = self.pipeline_state.find_one(
            {"status": "completed"},
            sort=[("timestamp", -1)]
        )
        if last and "timestamp" in last:
            ts = last["timestamp"]
            return ts.strftime("%Y/%m/%d")
        return None

    def get_stats(self) -> Dict:
        """Return current database statistics."""
        return {
            "total_papers": self.papers.count_documents({}),
            "total_drugs": self.drugs.count_documents({}),
            "total_sub_mechanisms": self.sub_mechanisms.count_documents({}),
            "total_categories": self.categories.count_documents({}),
            "total_runs": self.pipeline_state.count_documents({}),
        }

    def close(self):
        """Close the MongoDB connection."""
        self.client.close()
