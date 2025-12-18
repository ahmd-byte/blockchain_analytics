"""
Wallet Data Ingestion Pipeline.

This script extracts wallet information from transaction data and enriches it
with on-chain data (balances, transaction counts) from Etherscan.

Features:
- Extract unique wallets from raw transactions
- Enrich with current balances
- Calculate basic statistics
- Idempotent execution with deduplication

Usage:
    python ingest_wallets.py --from-transactions
    python ingest_wallets.py --addresses 0x... 0x...
"""

import argparse
from datetime import datetime, timezone
from typing import Dict, List, Set
from collections import defaultdict

from google.cloud import bigquery

from .config import CONFIG
from .utils import (
    setup_logger,
    BigQueryHelper,
    CheckpointManager,
    normalize_address,
    wei_to_ether,
    generate_record_hash,
)
from .etherscan_client import EtherscanClient


# Pipeline constants
PIPELINE_NAME = "wallet_ingestion"


# BigQuery schema for raw wallets
RAW_WALLETS_SCHEMA = [
    bigquery.SchemaField("wallet_address", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("first_seen_timestamp", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("last_seen_timestamp", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("balance_wei", "NUMERIC", mode="NULLABLE"),  # NUMERIC for large values
    bigquery.SchemaField("balance_eth", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("total_transactions_in", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("total_transactions_out", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("total_value_in_wei", "NUMERIC", mode="NULLABLE"),  # NUMERIC for large values
    bigquery.SchemaField("total_value_out_wei", "NUMERIC", mode="NULLABLE"),  # NUMERIC for large values
    bigquery.SchemaField("total_value_in_eth", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("total_value_out_eth", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("unique_counterparties", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("is_contract", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("record_hash", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]


class WalletIngestionPipeline:
    """
    Pipeline for ingesting and enriching wallet data.
    
    This pipeline extracts wallet information from transaction data,
    enriches it with on-chain data, and loads it into BigQuery.
    """
    
    def __init__(
        self,
        etherscan_client: EtherscanClient = None,
        bq_helper: BigQueryHelper = None,
        checkpoint_manager: CheckpointManager = None
    ):
        """
        Initialize the wallet ingestion pipeline.
        
        Args:
            etherscan_client: Etherscan API client
            bq_helper: BigQuery helper
            checkpoint_manager: Checkpoint manager
        """
        self.logger = setup_logger(__name__)
        self.etherscan = etherscan_client or EtherscanClient()
        self.bq = bq_helper or BigQueryHelper()
        self.checkpoint = checkpoint_manager or CheckpointManager(self.bq)
        
        # Configuration
        self.raw_dataset = CONFIG.bigquery.raw_dataset
        self.raw_table = CONFIG.bigquery.raw_wallets_table
        self.raw_transactions_table = CONFIG.bigquery.raw_transactions_table
        
        # Statistics
        self.stats = {
            "total_wallets_processed": 0,
            "new_wallets_inserted": 0,
            "wallets_updated": 0,
            "errors": 0
        }
    
    def extract_wallets_from_transactions(
        self,
        limit: int = None,
        min_transactions: int = 1
    ) -> List[Dict]:
        """
        Extract unique wallet addresses and statistics from raw transactions.
        
        Args:
            limit: Maximum number of wallets to process
            min_transactions: Minimum transaction count to include wallet
            
        Returns:
            List[Dict]: List of wallet statistics
        """
        self.logger.info("Extracting wallets from raw transactions...")
        
        # Check if transactions table exists
        if not self.bq.table_exists(self.raw_dataset, self.raw_transactions_table):
            self.logger.warning("Raw transactions table not found")
            return []
        
        # Query to extract wallet statistics from transactions
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        query = f"""
        WITH wallet_stats AS (
            SELECT
                wallet_address,
                MIN(transaction_timestamp) as first_seen_timestamp,
                MAX(transaction_timestamp) as last_seen_timestamp,
                SUM(CASE WHEN direction = 'in' THEN 1 ELSE 0 END) as total_transactions_in,
                SUM(CASE WHEN direction = 'out' THEN 1 ELSE 0 END) as total_transactions_out,
                SUM(CASE WHEN direction = 'in' THEN value_wei ELSE 0 END) as total_value_in_wei,
                SUM(CASE WHEN direction = 'out' THEN value_wei ELSE 0 END) as total_value_out_wei,
                COUNT(DISTINCT counterparty) as unique_counterparties
            FROM (
                -- Incoming transactions (wallet is to_address)
                SELECT
                    to_address as wallet_address,
                    from_address as counterparty,
                    'in' as direction,
                    value_wei,
                    transaction_timestamp
                FROM `{self.bq.project_id}.{self.raw_dataset}.{self.raw_transactions_table}`
                WHERE to_address IS NOT NULL AND to_address != ''
                
                UNION ALL
                
                -- Outgoing transactions (wallet is from_address)
                SELECT
                    from_address as wallet_address,
                    to_address as counterparty,
                    'out' as direction,
                    value_wei,
                    transaction_timestamp
                FROM `{self.bq.project_id}.{self.raw_dataset}.{self.raw_transactions_table}`
                WHERE from_address IS NOT NULL AND from_address != ''
            )
            GROUP BY wallet_address
            HAVING (total_transactions_in + total_transactions_out) >= {min_transactions}
        )
        SELECT * FROM wallet_stats
        ORDER BY (total_transactions_in + total_transactions_out) DESC
        {limit_clause}
        """
        
        results = self.bq.execute_query(query)
        self.logger.info(f"Extracted {len(results)} unique wallets")
        
        return results
    
    def enrich_wallet_with_balance(self, wallet_address: str) -> Dict:
        """
        Fetch current balance for a wallet address.
        
        Args:
            wallet_address: Ethereum address
            
        Returns:
            Dict: Balance information
        """
        try:
            balance_wei = self.etherscan.get_address_balance(wallet_address)
            return {
                "balance_wei": balance_wei,
                "balance_eth": wei_to_ether(balance_wei)
            }
        except Exception as e:
            self.logger.warning(f"Error fetching balance for {wallet_address}: {e}")
            return {"balance_wei": None, "balance_eth": None}
    
    def enrich_wallets_batch(self, addresses: List[str]) -> Dict[str, Dict]:
        """
        Fetch balances for multiple wallet addresses in batch.
        
        Args:
            addresses: List of Ethereum addresses (max 20)
            
        Returns:
            Dict: Map of address to balance info
        """
        result = {}
        
        # Process in batches of 20 (Etherscan limit)
        for i in range(0, len(addresses), 20):
            batch = addresses[i:i + 20]
            try:
                balances = self.etherscan.get_multi_address_balance(batch)
                for item in balances:
                    addr = normalize_address(item.get("account", ""))
                    balance_wei = int(item.get("balance", "0"))
                    result[addr] = {
                        "balance_wei": balance_wei,
                        "balance_eth": wei_to_ether(balance_wei)
                    }
            except Exception as e:
                self.logger.warning(f"Error fetching batch balances: {e}")
                # Fall back to individual requests
                for addr in batch:
                    result[addr] = self.enrich_wallet_with_balance(addr)
        
        return result
    
    def _get_existing_wallets(self, addresses: List[str]) -> Set[str]:
        """
        Check which wallet addresses already exist in BigQuery.
        
        Args:
            addresses: List of wallet addresses to check
            
        Returns:
            Set: Set of existing wallet addresses
        """
        if not addresses:
            return set()
        
        if not self.bq.table_exists(self.raw_dataset, self.raw_table):
            return set()
        
        address_list = ", ".join(f"'{a}'" for a in addresses)
        query = f"""
        SELECT DISTINCT wallet_address
        FROM `{self.bq.project_id}.{self.raw_dataset}.{self.raw_table}`
        WHERE wallet_address IN ({address_list})
        """
        
        results = self.bq.execute_query(query)
        return {row["wallet_address"] for row in results}
    
    def _transform_wallet(
        self,
        wallet_stats: Dict,
        balance_info: Dict = None,
        is_contract: bool = None
    ) -> Dict:
        """
        Transform wallet statistics to BigQuery format.
        
        Args:
            wallet_stats: Wallet statistics from extraction
            balance_info: Balance information from enrichment
            is_contract: Whether the address is a contract
            
        Returns:
            Dict: Transformed wallet record
        """
        balance_info = balance_info or {}
        
        wallet_address = normalize_address(wallet_stats.get("wallet_address", ""))
        total_value_in_wei = int(wallet_stats.get("total_value_in_wei", 0) or 0)
        total_value_out_wei = int(wallet_stats.get("total_value_out_wei", 0) or 0)
        
        record_hash = generate_record_hash(
            wallet_address,
            wallet_stats.get("first_seen_timestamp"),
            wallet_stats.get("last_seen_timestamp"),
            wallet_stats.get("total_transactions_in", 0),
            wallet_stats.get("total_transactions_out", 0)
        )
        
        # Handle timestamp formatting
        first_seen = wallet_stats.get("first_seen_timestamp")
        last_seen = wallet_stats.get("last_seen_timestamp")
        
        if first_seen and hasattr(first_seen, "isoformat"):
            first_seen = first_seen.isoformat()
        if last_seen and hasattr(last_seen, "isoformat"):
            last_seen = last_seen.isoformat()
        
        return {
            "wallet_address": wallet_address,
            "first_seen_timestamp": first_seen,
            "last_seen_timestamp": last_seen,
            "balance_wei": str(balance_info.get("balance_wei")) if balance_info.get("balance_wei") else None,
            "balance_eth": balance_info.get("balance_eth"),
            "total_transactions_in": int(wallet_stats.get("total_transactions_in", 0) or 0),
            "total_transactions_out": int(wallet_stats.get("total_transactions_out", 0) or 0),
            "total_value_in_wei": str(total_value_in_wei),  # String for NUMERIC type
            "total_value_out_wei": str(total_value_out_wei),  # String for NUMERIC type
            "total_value_in_eth": wei_to_ether(total_value_in_wei),
            "total_value_out_eth": wei_to_ether(total_value_out_wei),
            "unique_counterparties": int(wallet_stats.get("unique_counterparties", 0) or 0),
            "is_contract": is_contract,
            "record_hash": record_hash,
            "source": "transaction_extraction",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
    
    def run_from_transactions(
        self,
        limit: int = None,
        min_transactions: int = 1,
        enrich_balances: bool = True,
        skip_existing: bool = True
    ) -> Dict:
        """
        Run wallet ingestion from existing transaction data.
        
        Args:
            limit: Maximum number of wallets to process
            min_transactions: Minimum transactions to include wallet
            enrich_balances: Whether to fetch current balances
            skip_existing: Whether to skip wallets already in table
            
        Returns:
            Dict: Ingestion statistics
        """
        self.logger.info("Starting wallet ingestion from transactions")
        
        # Extract wallets from transactions
        wallet_stats = self.extract_wallets_from_transactions(
            limit=limit,
            min_transactions=min_transactions
        )
        
        if not wallet_stats:
            self.logger.info("No wallets to process")
            return self.stats
        
        # Filter existing wallets if needed
        addresses = [normalize_address(w["wallet_address"]) for w in wallet_stats]
        
        if skip_existing:
            existing = self._get_existing_wallets(addresses)
            wallet_stats = [
                w for w in wallet_stats
                if normalize_address(w["wallet_address"]) not in existing
            ]
            self.logger.info(f"Filtered to {len(wallet_stats)} new wallets")
        
        if not wallet_stats:
            self.logger.info("All wallets already exist")
            return self.stats
        
        # Enrich with balances
        balance_map = {}
        if enrich_balances:
            addresses_to_enrich = [
                normalize_address(w["wallet_address"])
                for w in wallet_stats
            ]
            self.logger.info(f"Enriching {len(addresses_to_enrich)} wallets with balances")
            balance_map = self.enrich_wallets_batch(addresses_to_enrich)
        
        # Transform and insert
        transformed = []
        for wallet in wallet_stats:
            addr = normalize_address(wallet["wallet_address"])
            balance_info = balance_map.get(addr, {})
            transformed.append(self._transform_wallet(wallet, balance_info))
        
        self.stats["total_wallets_processed"] = len(transformed)
        
        # Insert in batches
        batch_size = CONFIG.bigquery.batch_size
        for i in range(0, len(transformed), batch_size):
            batch = transformed[i:i + batch_size]
            try:
                inserted = self.bq.insert_rows(
                    dataset_id=self.raw_dataset,
                    table_id=self.raw_table,
                    rows=batch,
                    schema=RAW_WALLETS_SCHEMA
                )
                self.stats["new_wallets_inserted"] += inserted
            except Exception as e:
                self.logger.error(f"Error inserting wallet batch: {e}")
                self.stats["errors"] += 1
        
        self.logger.info(f"Wallet ingestion complete. Stats: {self.stats}")
        return self.stats
    
    def run_for_addresses(
        self,
        addresses: List[str],
        enrich_balances: bool = True
    ) -> Dict:
        """
        Run wallet ingestion for specific addresses.
        
        Args:
            addresses: List of Ethereum addresses
            enrich_balances: Whether to fetch current balances
            
        Returns:
            Dict: Ingestion statistics
        """
        self.logger.info(f"Starting wallet ingestion for {len(addresses)} addresses")
        
        # Normalize addresses
        addresses = [normalize_address(a) for a in addresses]
        
        # Check existing
        existing = self._get_existing_wallets(addresses)
        new_addresses = [a for a in addresses if a not in existing]
        
        if not new_addresses:
            self.logger.info("All addresses already exist")
            return self.stats
        
        # Enrich with balances
        balance_map = {}
        if enrich_balances:
            balance_map = self.enrich_wallets_batch(new_addresses)
        
        # Create wallet records
        transformed = []
        for addr in new_addresses:
            balance_info = balance_map.get(addr, {})
            wallet_stats = {
                "wallet_address": addr,
                "first_seen_timestamp": None,
                "last_seen_timestamp": None,
                "total_transactions_in": 0,
                "total_transactions_out": 0,
                "total_value_in_wei": 0,
                "total_value_out_wei": 0,
                "unique_counterparties": 0,
            }
            transformed.append(self._transform_wallet(wallet_stats, balance_info))
        
        self.stats["total_wallets_processed"] = len(transformed)
        
        # Insert
        if transformed:
            try:
                inserted = self.bq.insert_rows(
                    dataset_id=self.raw_dataset,
                    table_id=self.raw_table,
                    rows=transformed,
                    schema=RAW_WALLETS_SCHEMA
                )
                self.stats["new_wallets_inserted"] = inserted
            except Exception as e:
                self.logger.error(f"Error inserting wallets: {e}")
                self.stats["errors"] += 1
        
        self.logger.info(f"Wallet ingestion complete. Stats: {self.stats}")
        return self.stats


def main():
    """Main entry point for CLI execution."""
    parser = argparse.ArgumentParser(
        description="Ingest wallet data into BigQuery"
    )
    parser.add_argument(
        "--from-transactions",
        action="store_true",
        help="Extract wallets from existing transaction data"
    )
    parser.add_argument(
        "--addresses",
        nargs="+",
        help="Specific addresses to ingest"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum wallets to process"
    )
    parser.add_argument(
        "--min-transactions",
        type=int,
        default=1,
        help="Minimum transactions for extraction"
    )
    parser.add_argument(
        "--no-enrich",
        action="store_true",
        help="Skip balance enrichment"
    )
    parser.add_argument(
        "--include-existing",
        action="store_true",
        help="Process existing wallets"
    )
    
    args = parser.parse_args()
    
    pipeline = WalletIngestionPipeline()
    
    if args.from_transactions:
        stats = pipeline.run_from_transactions(
            limit=args.limit,
            min_transactions=args.min_transactions,
            enrich_balances=not args.no_enrich,
            skip_existing=not args.include_existing
        )
    elif args.addresses:
        stats = pipeline.run_for_addresses(
            addresses=args.addresses,
            enrich_balances=not args.no_enrich
        )
    else:
        parser.error("Specify --from-transactions or --addresses")
    
    print(f"\nWallet Ingestion Statistics:")
    print(f"  Total Processed: {stats['total_wallets_processed']}")
    print(f"  New Inserted: {stats['new_wallets_inserted']}")
    print(f"  Updated: {stats['wallets_updated']}")
    print(f"  Errors: {stats['errors']}")


if __name__ == "__main__":
    main()

