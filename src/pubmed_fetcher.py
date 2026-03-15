"""
PubMed Fetcher — queries NCBI E-utilities to find and download autism pharmacology papers.

Uses the public E-utilities API:
  - esearch: find PMIDs matching a query
  - efetch: retrieve paper metadata (title, authors, abstract, journal, etc.)

Rate-limited to 3 requests/second per NCBI guidelines.
"""

import time
import logging
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)

ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


@dataclass
class PubMedPaper:
    """Raw paper data from PubMed before LLM analysis."""
    pmid: str
    title: str
    abstract: str
    authors: str
    journal: str
    year: int
    doi: str
    url: str  # PubMed URL


class PubMedFetcher:
    def __init__(self, email: str, rate_limit_delay: float = 0.4):
        self.email = email
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": f"AutismPharmaKB/1.0 ({email})"})

    def search(self, query: str, max_results: int = 50, min_date: Optional[str] = None) -> List[str]:
        """
        Search PubMed and return a list of PMIDs.
        
        Args:
            query: PubMed search query string
            max_results: Maximum number of results to return
            min_date: Optional minimum date filter (YYYY/MM/DD format)
            
        Returns:
            List of PMID strings
        """
        params = {
            "db": "pubmed",
            "term": query,
            "retmax": max_results,
            "retmode": "json",
            "email": self.email,
            "sort": "date",  # newest first
        }
        if min_date:
            params["mindate"] = min_date
            params["datetype"] = "pdat"  # publication date

        try:
            time.sleep(self.rate_limit_delay)
            resp = self.session.get(ESEARCH_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            pmids = data.get("esearchresult", {}).get("idlist", [])
            logger.info(f"Found {len(pmids)} PMIDs for query: {query[:80]}...")
            return pmids
        except Exception as e:
            logger.error(f"PubMed search failed for query '{query[:60]}...': {e}")
            return []

    def fetch_papers(self, pmids: List[str]) -> List[PubMedPaper]:
        """
        Fetch full paper metadata for a list of PMIDs.
        
        Fetches in batches of 20 to stay within rate limits.
        """
        if not pmids:
            return []

        all_papers = []
        batch_size = 20

        for i in range(0, len(pmids), batch_size):
            batch = pmids[i:i + batch_size]
            try:
                time.sleep(self.rate_limit_delay)
                resp = self.session.get(EFETCH_URL, params={
                    "db": "pubmed",
                    "id": ",".join(batch),
                    "retmode": "xml",
                    "rettype": "abstract",
                    "email": self.email,
                }, timeout=60)
                resp.raise_for_status()

                papers = self._parse_xml(resp.text)
                all_papers.extend(papers)
                logger.info(f"Fetched {len(papers)} papers (batch {i // batch_size + 1})")

            except Exception as e:
                logger.error(f"Failed to fetch PMIDs batch starting at {i}: {e}")
                continue

        return all_papers

    def _parse_xml(self, xml_text: str) -> List[PubMedPaper]:
        """Parse PubMed XML response into PubMedPaper objects."""
        papers = []
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.error(f"XML parse error: {e}")
            return papers

        for article in root.findall(".//PubmedArticle"):
            try:
                paper = self._parse_article(article)
                if paper and paper.abstract:  # skip papers without abstracts
                    papers.append(paper)
            except Exception as e:
                logger.warning(f"Failed to parse an article: {e}")
                continue

        return papers

    def _parse_article(self, article) -> Optional[PubMedPaper]:
        """Parse a single PubmedArticle XML element."""
        medline = article.find(".//MedlineCitation")
        if medline is None:
            return None

        # PMID
        pmid_el = medline.find(".//PMID")
        pmid = pmid_el.text if pmid_el is not None else ""
        if not pmid:
            return None

        # Title
        title_el = medline.find(".//ArticleTitle")
        title = self._get_text(title_el)

        # Abstract
        abstract_parts = []
        for abs_text in medline.findall(".//AbstractText"):
            label = abs_text.get("Label", "")
            text = self._get_text(abs_text)
            if label:
                abstract_parts.append(f"{label}: {text}")
            else:
                abstract_parts.append(text)
        abstract = " ".join(abstract_parts)

        # Authors
        authors = []
        for author in medline.findall(".//Author"):
            last = author.findtext("LastName", "")
            first = author.findtext("ForeName", "")
            initials = author.findtext("Initials", "")
            if last:
                name = f"{last} {initials}" if initials else last
                authors.append(name)
        authors_str = ", ".join(authors[:3])
        if len(authors) > 3:
            authors_str += " et al."

        # Journal
        journal = medline.findtext(".//Journal/Title", "")
        if not journal:
            journal = medline.findtext(".//MedlineJournalInfo/MedlineTA", "")

        # Year
        year = 0
        year_el = medline.find(".//PubDate/Year")
        if year_el is not None and year_el.text:
            try:
                year = int(year_el.text)
            except ValueError:
                pass
        if year == 0:
            medline_date = medline.findtext(".//PubDate/MedlineDate", "")
            if medline_date:
                try:
                    year = int(medline_date[:4])
                except ValueError:
                    pass

        # DOI
        doi = ""
        for eid in article.findall(".//ArticleIdList/ArticleId"):
            if eid.get("IdType") == "doi":
                doi = eid.text or ""
                break

        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

        return PubMedPaper(
            pmid=pmid,
            title=title,
            abstract=abstract,
            authors=authors_str,
            journal=journal,
            year=year,
            doi=doi,
            url=url,
        )

    def _get_text(self, element) -> str:
        """Extract all text content from an element, including mixed content."""
        if element is None:
            return ""
        # itertext() gets all text including text in child elements
        return "".join(element.itertext()).strip()

    def search_and_fetch(self, query: str, max_results: int = 50,
                         min_date: Optional[str] = None) -> List[PubMedPaper]:
        """Convenience: search + fetch in one call."""
        pmids = self.search(query, max_results=max_results, min_date=min_date)
        if not pmids:
            return []
        return self.fetch_papers(pmids)
