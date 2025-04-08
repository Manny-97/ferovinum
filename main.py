import logging
import os
import re
import time
import json
from glob import glob
import pandas as pd
from datetime import datetime


# Create outputs directory if not exists
os.makedirs("outputs", exist_ok=True)

log_filename = datetime.now().strftime("outputs/log_%Y-%m-%d_%H-%M-%S.log")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler() 
    ]
)

logger = logging.getLogger(__name__)


def parse_logs(log_dir):
    """
    Parses log files from the specified directory into a pandas DataFrame.

    Args:
        log_dir (str): Directory containing the log files.

    Returns:
        pd.DataFrame: DataFrame containing the parsed log entries.
    """
    log_files = sorted(
        glob(os.path.join(log_dir, "log_*.txt")),
        key=lambda x: int(re.search(r"log_(\d+).txt", x).group(1))
    )
    print(f"The following files are the available log files: {log_files}")
    full_text = ""
    for file in log_files:
        with open(file, "r", encoding="utf-8") as f:
            file_content = f.read()
            if full_text and not full_text.endswith("\n"):
                full_text += file_content
            else:
                full_text += "\n" + file_content

    output_file_path = 'merged_logs.txt'
    with open(output_file_path, 'w', encoding='utf-8') as output_file:
        output_file.write(full_text)
    logger.info(f"Merged logs written to {output_file_path}")

    entry_start_pattern = re.compile(
        r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| "
        r"(ORDER|RESULT|TRANSACTION|RESPONSE) \|",
        re.MULTILINE
    )

    matches = list(entry_start_pattern.finditer(full_text))
    logger.info(f"Detected {len(matches)} log entries.")

    entries = []
    for i in range(len(matches)):
        start = matches[i].start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(full_text)
        entry = full_text[start:end].strip().replace("\n", " ")
        entries.append(entry)

    records = []
    for entry in entries:
        parts = entry.split(" | ", 3)
        if len(parts) == 4:
            records.append(parts)
        else:
            logger.info(f"[WARN] Incomplete log entry skipped: {entry[:50]}...")

    df = pd.DataFrame(records, columns=["timestamp", "message_type", "trace_id", "details"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    logger.info("Parsed logs successfully into DataFrame.")
    
    return df


def read_skus(sku_dir):
    """
    Reads SKU metadata from a JSON file and returns it as a DataFrame.

    Args:
        sku_dir (str): Directory containing the SKU JSON file.

    Returns:
        pd.DataFrame: DataFrame containing SKU metadata.
    """
    logger.info("Starting to load SKU data.")

    # Construct the path to the SKU JSON file
    sku_file = os.path.join(sku_dir, "skus.json")
    if not os.path.exists(sku_file):
        logger.error(f"SKU file not found at {sku_file}.")
        return pd.DataFrame()

    # Load the JSON data
    try:
        with open(sku_file, "r", encoding="utf-8") as f:
            sku_data = json.load(f)
        logger.info("Successfully loaded SKU JSON data.")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")


    sku_df = pd.json_normalize(sku_data, sep="_")
    logger.info("Normalized JSON data into a flat DataFrame.")

    sku_df.rename(columns={"code": "sku"}, inplace=True)
    sku_df.rename(
        columns={
            "attribute_json_grain_variety": "grain_variety",
            "attribute_json_grape_variety": "grape_variety",
            "attribute_json_blend": "blend",
            "attribute_json_year": "year",
            "attribute_json_age": "age",
            "attribute_json_abv": "abv",
            "attribute_json_bottle_ml": "bottle_ml",
            "region_country_id": "country_id",
            "region_country_name": "country_name",
            "attribute_json_barrel_type": "barrel_type"
        },
        inplace=True
    )
    logger.info("Renamed columns for clarity.")

    return sku_df


def read_market_prices(path):
    """
    Reads market price data from parquet files and returns it as a DataFrame.

    Args:
        path (str): Directory containing the market price parquet files.

    Returns:
        pd.DataFrame: DataFrame containing market price data.
    """
    logger.info("Starting to load market data from parquet files.")
    all_files = glob(os.path.join(path, "market_prices_*.parquet"))
    logger.info(f"Found {len(all_files)} parquet files. Reading and concatenating them.")

    try:
        market_data = pd.concat(
            (pd.read_parquet(f, engine='pyarrow') for f in all_files),
            ignore_index=True
        )
    except Exception as e:
        logger.error(f"Error reading parquet files: {e}")
        return pd.DataFrame()
    
    logger.info("Successfully read and concatenated parquet files.")

    market_data["timestamp"] = pd.to_datetime(market_data["timestamp"])
    market_data["sku"] = market_data["quote_id"].str.extract(r"^(.*?)-\d+$")
    market_data.to_csv("outputs/market_data.csv", index=False)
    logger.info(f"Saved processed market data to outputs/market_data.csv.")
    return market_data


def extract_transactions(logs):
    """
    Extracts transaction data from logs and returns it as a DataFrame.

    Args:
        logs (pd.DataFrame): DataFrame containing log entries.

    Returns:
        pd.DataFrame: DataFrame containing extracted transaction data.
    """
    logs["timestamp"] = pd.to_datetime(logs["timestamp"])

    orders = logs[logs.message_type == "ORDER"].copy()
    orders[["action", "sku", "quantity"]] = orders["details"].str.extract(
        r"(buy|sell)\s+(\S+)\s+(\d+)"
    )
    logger.info("Extracted 'action', 'sku', and 'quantity' from order details.")

    orders.dropna(subset=["sku", "quantity"], inplace=True)
    orders["quantity"] = orders["quantity"].astype(int)

    logger.info(f"Adjusted quantity for buy orders.")
    orders.loc[orders["action"] == "buy", "quantity"] *= -1

    transactions = logs[logs.message_type == "TRANSACTION"].copy()
    transactions["transaction_amount"] = transactions["details"]
    transactions = transactions[["trace_id", "transaction_amount"]]

    merged = orders.merge(transactions, on="trace_id", how="inner")

    return merged


def enrich_transactions(transactions, sku_df, prices):
    """
    Enriches transaction data with SKU metadata and market prices.

    Args:
        transactions (pd.DataFrame): DataFrame containing transaction data.
        sku_df (pd.DataFrame): DataFrame containing SKU metadata.
        prices (pd.DataFrame): DataFrame containing market price data.

    Returns:
        pd.DataFrame: DataFrame containing enriched transaction data.
    """
    enriched = transactions.merge(sku_df, on="sku", how="left")
    logger.info("Merged transactions with SKU metadata.")
    prices = prices.rename(
        columns={
            "sku": "sku",
            "timestamp": "price_time",
            "price_usd": "market_price"
        }
    )

    logger.info("Renamed price columns for clarity.")
    enriched = enriched.sort_values(['sku', 'timestamp'])
    prices = prices.sort_values(['sku', 'price_time'])

    logger.info("Sorted dataframes by SKU and time for asof merge.")
    enriched = pd.merge_asof(
        enriched.sort_values('timestamp'),
        prices[['sku', 'price_time', 'market_price']].sort_values("price_time"),
        by="sku",
        left_on="timestamp",
        right_on="price_time",
        direction="backward",
        allow_exact_matches=True
    )
    logger.info("Performed asof merge to attach market prices.")

    enriched = enriched.sort_values(["trace_id", "price_time"], ascending=[True, False])

    enriched["transaction_value"] = enriched["quantity"] * enriched["market_price"]
    enriched["year"] = enriched["timestamp"].dt.year
    enriched["quarter"] = enriched["timestamp"].dt.to_period("Q")
    enriched["week"] = enriched["timestamp"].dt.isocalendar().week
    enriched.drop(
        axis=1,
        columns=["transaction_amount", "id", "details"],
        inplace=True
    )
    logger.info("Dropped unnecessary columns.")

    enriched.to_csv("outputs/final_clean_dataset.csv", index=False)
    logger.info("Saved the enriched dataset to 'outputs/final_clean_dataset.csv'.")

    return enriched

def transaction_volume_by_region(enriched):
    """
    Identifies the region with the highest transaction volume per SKU
    for each year and quarter.

    Args:
        enriched (pd.DataFrame): DataFrame containing transaction data
                                 including SKU, region, year, quarter,
                                 and quantity.

    Returns:
        pd.DataFrame: DataFrame with top region per SKU for each quarter.
    """
    try:
        logger.info("Grouping data by SKU, year, quarter, region ID, and region name...")
        grouped = enriched.groupby(
            ["sku", "year", "quarter", "region_id", "region_name"],
            as_index=False
        )["quantity"].sum()

        logger.info("Sorting grouped data by SKU, year, quarter, and quantity (descending)...")
        sorted_grouped = grouped.sort_values(
            by=["sku", "year", "quarter", "quantity"],
            ascending=[True, True, True, False]
        )
        logger.debug("Top 5 sorted entries:\n%s", sorted_grouped.head())

        logger.info("Dropping duplicates to find top region per SKU per quarter...")
        top_regions = sorted_grouped.drop_duplicates(
            subset=["sku", "year", "quarter"],
            keep="first"
        )

        logger.info("Saving top region per SKU per quarter to CSV...")
        top_regions.to_csv("outputs/top_region_per_sku.csv", index=False)

        return top_regions

    except Exception as e:
        logger.exception("Failed to compute transaction volume by region.")


def most_profitable_brands(enriched):
    """
    Identifies the two most profitable brands based on sales
    in early 2024 (up to week 21).

    Args:
        enriched (pd.DataFrame): DataFrame containing sales transactions
                                 with 'brand_name', 'sku', and
                                 'transaction_value'.

    Returns:
        pd.DataFrame: Top 2 most profitable brands.
    """
    try:
        logger.info("Filtering transactions for sales in the first 21 weeks of 2024...")
        early_2024 = enriched[
            (enriched["year"] == 2024)
            & (enriched["week"] <= 21)
            & (enriched["action"] == "sell")
        ]
        logger.debug(f"Filtered data shape: {early_2024.shape}")

        logger.info("Calculating total transaction value per brand...")
        brand_profits = early_2024.groupby(
            ["brand_id", "brand_name", "sku"]
        )["transaction_value"].sum().reset_index()
        logger.debug(f"Grouped data shape: {brand_profits.shape}")

        logger.info("Sorting brands by total transaction value...")
        top = brand_profits.sort_values(
            by="transaction_value", ascending=False
        )
        logger.debug("Top 5 most profitable brand entries:\n%s", top.head())

        logger.info("Selecting top 2 most profitable brands...")
        top2 = top.head(2)
        logger.debug(f"Top 2 brands:\n{top2}")

        formatted_string = (
            "The top two most profitable brands are "
            f"{' and '.join(top2['brand_name'])} with total revenues of $"
            f"{' and $'.join(map(str, top2['transaction_value']))}."
        )
        logger.info(formatted_string)
        print(formatted_string)

        logger.info("Saving top 2 and all brands' profitability to CSVs...")
        top2.to_csv("outputs/two_most_profitable_brands.csv", index=False)
        top.to_csv("outputs/most_profitable_brands.csv", index=False)

        return top2

    except Exception as e:
        logger.exception("An error occurred while identifying the most profitable brands.")


def generate_test_case():
    """
    Filters a transaction dataset to prepare a test case for
    ending inventory analysis on a specific set of SKUs.

    Reads from a cleaned CSV dataset and filters all entries before
    2025-02-01. Aggregates quantity by SKU and outputs the ending inventory
    for a select list of SKUs.

    Returns:
        None
    """
    try:
        logger.info("Loading cleaned dataset from CSV...")
        df = pd.read_csv("outputs/final_clean_dataset.csv")
        logger.debug(f"Initial dataset shape: {df.shape}")

        end_date = "2025-02-01"
        logger.info(f"Filtering rows before end date: {end_date}")
        mask = df["timestamp"] < end_date
        df = df[mask]
        logger.debug(f"Filtered dataset shape: {df.shape}")

        logger.info("Grouping by SKU to calculate total quantity...")
        new_df = df.groupby("sku", as_index=False)["quantity"].sum()
        new_df.rename(columns={"quantity": "ending inventory"}, inplace=True)
        logger.debug("Renamed 'quantity' column to 'ending inventory'.")

        filter_sku = [
            "WINE-OPU-001", "WINE-OPU-003", "WINE-LAF-004",
            "WHKY-GLE-018", "BRBN-MAK-024"
        ]
        logger.info(f"Filtering for specific SKUs: {filter_sku}")
        filtered_df = new_df[new_df["sku"].isin(filter_sku)].copy()
        filtered_df["sku"] = pd.Categorical(
            filtered_df["sku"],
            categories=filter_sku,
            ordered=True
        )
        logger.debug(f"Final filtered DataFrame:\n{filtered_df.sort_values('sku')}")
        print(filtered_df.sort_values("sku"))

    except Exception as e:
        logger.exception("Failed to generate test case.")

if __name__ == "__main__":
    logger.info("Pipeline started.")

    try:
        logger.debug("Reading logs...")
        logs = parse_logs("data/logs")
        logger.debug("Reading SKUs...")
        sku_df = read_skus("data/skus")
        logger.debug("Reading market prices...")
        prices = read_market_prices("data/market_prices")

        logger.info("Extracting transactions...")
        tx = extract_transactions(logs)

        logger.info("Enriching transactions...")
        enriched = enrich_transactions(tx, sku_df, prices)

        logger.info("Analyzing transaction volume by region...")
        transaction_volume_by_region(enriched)

        logger.info("Finding most profitable brands...")
        most_profitable_brands(enriched)

        logger.info("Generating test case data...")
        generate_test_case()
        logger.info("Now you can check all directory and files......")
        time.sleep(15)
        logger.info("Pipeline finished successfully.")
    except Exception as e:
        logger.exception("Pipeline failed with an error:")