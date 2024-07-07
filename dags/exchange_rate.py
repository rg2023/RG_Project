import httpx
from datetime import datetime

class ExchangeRateData:
    def __init__(self, documentation, terms_of_use, time_last_update_unix, time_last_update_utc,
                 time_next_update_unix, time_next_update_utc, base_code, conversion_rates):
        self.documentation = documentation
        self.terms_of_use = terms_of_use
        self.time_last_update_unix = time_last_update_unix
        self.time_last_update_utc = time_last_update_utc
        self.time_next_update_unix = time_next_update_unix
        self.time_next_update_utc = time_next_update_utc
        self.base_code = base_code
        self.conversion_rates = conversion_rates

    def to_dict(self):
        return {
            "documentation": self.documentation,
            "terms_of_use": self.terms_of_use,
            "time_last_update_unix": self.time_last_update_unix,
            "time_last_update_utc": self.time_last_update_utc.strftime('%Y-%m-%d %H:%M:%S'),
            "time_next_update_unix": self.time_next_update_unix,
            "time_next_update_utc": self.time_next_update_utc.strftime('%Y-%m-%d %H:%M:%S'),
            "base_code": self.base_code,
            "conversion_rates": self.conversion_rates
        }

async def get_exchange_rates(base_currency: str) -> ExchangeRateData:
    api_key = "4e61cf72d3ae31c331eeed6a"
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/{base_currency}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        if response.status_code == 200:
            data = response.json()
            return ExchangeRateData(
                documentation=data["documentation"],
                terms_of_use=data["terms_of_use"],
                time_last_update_unix=data["time_last_update_unix"],
                time_last_update_utc=datetime.strptime(data["time_last_update_utc"], '%a, %d %b %Y %H:%M:%S %z'),
                time_next_update_unix=data["time_next_update_unix"],
                time_next_update_utc=datetime.strptime(data["time_next_update_utc"], '%a, %d %b %Y %H:%M:%S %z'),
                base_code=data["base_code"],
                conversion_rates=data["conversion_rates"]
            )
        else:
            raise Exception("Failed to fetch data from API")
