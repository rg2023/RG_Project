import pandas as pd
from exchange_rate import ExchangeRateData

def process_exchange_rate_data(exchange_rate_data: ExchangeRateData) -> pd.DataFrame:
    # Create DataFrame from filtered rates
    conversion_rates_df = pd.DataFrame.from_dict(exchange_rate_data.conversion_rates, orient='index', columns=['rate'])
    # Add base_currency column
    conversion_rates_df['base_currency'] = exchange_rate_data.base_code
    # Add running ID column
    conversion_rates_df['id'] = range(1, len(conversion_rates_df) + 1)
    return conversion_rates_df
