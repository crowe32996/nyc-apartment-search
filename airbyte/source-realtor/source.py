from airbyte_cdk.sources import Source
from airbyte_cdk.models import AirbyteCatalog, AirbyteStream, ConfiguredAirbyteCatalog, SyncMode
from airbyte_cdk.sources.streams import Stream
import http.client
import json

RESULTS_PER_PAGE = 200
MAX_PAGES = 100  # safety limit

class ApartmentsStream(Stream):
    def __init__(self, config, **kwargs):
        super().__init__(**kwargs)
        self.api_key = config["api_key"]
        self.api_host = "realtor-search.p.rapidapi.com"

    def read_records(self, **kwargs):
        for page in range(1, MAX_PAGES + 1):
            conn = http.client.HTTPSConnection(self.api_host)
            headers = {
                "x-rapidapi-key": self.api_key,
                "x-rapidapi-host": self.api_host
            }
            endpoint = (
                f"/properties/search-rent"
                f"?location=postal_code%3A11201"
                f"&resultsPerPage={RESULTS_PER_PAGE}"
                f"&sortBy=best_match"
                f"&expandSearchArea=5"
                f"&homeSize=300"
                f"&page={page}"
            )
            try:
                conn.request("GET", endpoint, headers=headers)
                res = conn.getresponse()
                if res.status != 200:
                    print(f"API returned {res.status} on page {page}")
                    break
                raw_data = res.read()
                json_data = json.loads(raw_data)
                results = json_data.get("data", {}).get("results", [])
                if not results:
                    break
                for record in results:
                    yield record
                if len(results) < RESULTS_PER_PAGE:
                    break
            finally:
                conn.close()

class SourceRealtor(Source):
    def check_connection(self, logger, config) -> tuple[bool, any]:
        try:
            # Quick test call
            stream = ApartmentsStream(config)
            records = list(stream.read_records())
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: dict) -> list[Stream]:
        return [ApartmentsStream(config)]