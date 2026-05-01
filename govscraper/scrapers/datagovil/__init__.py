"""data.gov.il (CKAN) scraper."""
from .ckan_client import CkanClient, CkanError
from .scraper import DataGovIlScraper

__all__ = ["DataGovIlScraper", "CkanClient", "CkanError"]
