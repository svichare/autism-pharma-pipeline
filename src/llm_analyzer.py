"""
LLM Analyzer — uses OpenAI to extract structured pharmacological data from paper abstracts.

Given a paper's title and abstract, the LLM extracts:
  - Drug name(s)
  - Mechanism of action
  - Mechanism category (GABAergic, Glutamatergic, etc.)
  - Sub-mechanism
  - Study type (RCT, meta-analysis, etc.)
  - Sample size
  - Target symptoms
  - Results summary
  - Trial phase
  - Whether the paper is relevant to autism pharmacology
"""

import json
import logging
from typing import Dict, Optional, List

from openai import OpenAI

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are an expert neuropharmacology researcher specializing in autism spectrum disorder (ASD) pharmacotherapy.

Your task is to analyze a research paper's title and abstract and extract structured pharmacological data.

You MUST classify the mechanism into one of these categories:
- GABAergic System
- Glutamatergic System
- Serotonergic System
- Oxytocinergic System
- Dopaminergic / Antipsychotic
- Cholinergic System
- Adrenergic / Stimulant
- mTOR Pathway
- Neuroinflammation / Immune Modulation
- Cannabinoid System
- Gut-Brain Axis
- Hormonal / Metabolic
- Other Emerging Targets

For sub_mechanism, use concise pharmacological labels like:
- "NMDA Receptor Antagonism", "GABA-B Receptor Agonism", "NKCC1 Chloride Transporter Antagonism"
- "Selective Serotonin Reuptake Inhibitors (SSRIs)", "5-HT1A Partial Agonism"
- "Oxytocin Receptor Agonism", "Vasopressin V1a Receptor Antagonism"
- "D2/5-HT2A Antagonism (Atypical Antipsychotic)", "D2 Partial Agonism (Atypical Antipsychotic)"
- "mTORC1 Inhibition", "Nrf2/ARE Pathway Activation", "Endocannabinoid System Modulation"
- "Microbiota Transfer / Fecal Transplant", "Folate Pathway / Cerebral Folate Correction"
- "Antipurinergic Therapy (Cell Danger Response)", "IGF-1 Pathway / Synaptic Rescue"
- Or create a similar concise label if the mechanism doesn't fit the above.

Respond ONLY with valid JSON matching the schema below. No extra text."""

EXTRACTION_PROMPT = """Analyze this research paper and extract pharmacological data:

TITLE: {title}

ABSTRACT: {abstract}

Return a JSON object with these fields:
{{
  "is_relevant": true/false,        // Is this paper about pharmacological treatment for autism/ASD?
  "drug_name": "...",                // Primary drug/compound studied (use generic name)
  "mechanism_of_action": "...",      // Detailed description of how the drug works
  "mechanism_category": "...",       // One of the 13 categories listed above
  "sub_mechanism": "...",            // Concise pharmacological sub-mechanism label
  "study_type": "...",               // e.g. "Randomized Controlled Trial", "Meta-analysis", "Open-label", "Review", "Case series"
  "sample_size": "...",              // Number of participants, or "N/A" for reviews
  "target_symptoms": "...",          // What ASD symptoms were targeted
  "results_summary": "...",          // 2-3 sentence summary of key findings and efficacy
  "trial_phase": "...",              // Phase 1/2/3, or "N/A"
  "result_direction": "positive/negative/mixed"  // Overall outcome direction
}}

If the paper is NOT about pharmacological intervention for autism, set is_relevant to false and fill other fields with "N/A".
"""


class LLMAnalyzer:
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.client = OpenAI(api_key=api_key)
        self.model = model

    def analyze_paper(self, title: str, abstract: str) -> Optional[Dict]:
        """
        Analyze a single paper and return structured extraction.
        
        Returns None if the paper is not relevant or analysis fails.
        """
        if not abstract or len(abstract.strip()) < 50:
            logger.warning(f"Skipping paper with insufficient abstract: {title[:60]}...")
            return None

        prompt = EXTRACTION_PROMPT.format(title=title, abstract=abstract)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                max_tokens=800,
                response_format={"type": "json_object"},
            )

            content = response.choices[0].message.content
            result = json.loads(content)

            if not result.get("is_relevant", False):
                logger.info(f"Paper not relevant to autism pharmacology: {title[:60]}...")
                return None

            # Validate required fields
            required = ["drug_name", "mechanism_of_action", "mechanism_category",
                         "sub_mechanism", "study_type", "results_summary"]
            for field in required:
                if not result.get(field) or result[field] == "N/A":
                    if field in ("drug_name", "mechanism_category"):
                        logger.warning(f"Missing required field '{field}' for: {title[:60]}...")
                        return None

            logger.info(f"Analyzed: {title[:60]}... -> {result.get('drug_name', '?')} ({result.get('mechanism_category', '?')})")
            return result

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error for '{title[:60]}...': {e}")
            return None
        except Exception as e:
            logger.error(f"LLM analysis failed for '{title[:60]}...': {e}")
            return None

    def analyze_batch(self, papers: List[Dict]) -> List[Dict]:
        """
        Analyze a batch of papers. Each paper dict should have 'title' and 'abstract'.
        
        Returns list of successfully analyzed papers with extracted fields merged in.
        """
        results = []
        for i, paper in enumerate(papers):
            title = paper.get("title", "")
            abstract = paper.get("abstract", "")
            
            logger.info(f"Analyzing paper {i + 1}/{len(papers)}: {title[:60]}...")
            extraction = self.analyze_paper(title, abstract)
            
            if extraction:
                # Merge extraction into paper data
                merged = {**paper, **extraction}
                results.append(merged)

        logger.info(f"Analyzed {len(papers)} papers, {len(results)} were relevant")
        return results
