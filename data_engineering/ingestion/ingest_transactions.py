"""
Transaction Ingestion Pipeline.

This script ingests blockchain transactions from Etherscan API into BigQuery.
It supports:
- Incremental ingestion using checkpoints
- Batch processing with configurable sizes
- Deduplication using transaction hashes
- Idempotent execution

Usage:
    python ingest_transactions.py --addresses 0x... 0x... --start-block 0
    python ingest_transactions.py --addresses-file addresses.txt --resume
"""

import argparse
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pathlib import Path

from google.cloud import bigquery

from .config import CONFIG
from .utils import (
    setup_logger,
    BigQueryHelper,
    CheckpointManager,
    normalize_address,
    wei_to_ether,
    unix_to_datetime,
    generate_record_hash,
    parse_hex_to_int,
)
from .etherscan_client import EtherscanClient


# Pipeline constants
PIPELINE_NAME = "transaction_ingestion"
CHECKPOINT_KEY_LAST_BLOCK = "last_processed_block"
CHECKPOINT_KEY_LAST_ADDRESS = "last_processed_address"


# BigQuery schema for raw transactions
RAW_TRANSACTIONS_SCHEMA = [
    bigquery.SchemaField("transaction_hash", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("block_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("block_hash", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("transaction_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("from_address", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("to_address", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("value_wei", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("value_eth", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("gas", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("gas_price", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("gas_used", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("nonce", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("transaction_index", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("input_data", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("contract_address", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("is_error", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("txreceipt_status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("confirmations", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("method_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("function_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("record_hash", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]


class TransactionIngestionPipeline:
    """
    Pipeline for ingesting blockchain transactions.
    
    This pipeline fetches transaction data from Etherscan and loads it into
    BigQuery's raw layer with deduplication and checkpoint support.
    """
    
    def __init__(
        self,
        etherscan_client: EtherscanClient = None,
        bq_helper: BigQueryHelper = None,
        checkpoint_manager: CheckpointManager = None
    ):
        """
        Initialize the transaction ingestion pipeline.
        
        Args:
            etherscan_client: Etherscan API client
            bq_helper: BigQuery helper
            checkpoint_manager: Checkpoint manager for resumable processing
        """
        self.logger = setup_logger(__name__)
        self.etherscan = etherscan_client or EtherscanClient()
        self.bq = bq_helper or BigQueryHelper()
        self.checkpoint = checkpoint_manager or CheckpointManager(self.bq)
        
        # Configuration
        self.raw_dataset = CONFIG.bigquery.raw_dataset
        self.raw_table = CONFIG.bigquery.raw_transactions_table
        self.batch_size = CONFIG.ingestion.batch_size
        
        # Statistics
        self.stats = {
            "total_fetched": 0,
            "total_inserted": 0,
            "duplicates_skipped": 0,
            "errors": 0
        }
    
    def _transform_transaction(self, tx: Dict, source_address: str) -> Dict:
        """
        Transform raw Etherscan transaction to BigQuery format.
        
        Args:
            tx: Raw transaction from Etherscan
            source_address: Address used to fetch this transaction
            
        Returns:
            Dict: Transformed transaction record
        """
        # Parse values safely
        value_wei = parse_hex_to_int(tx.get("value", "0"))
        timestamp = int(tx.get("timeStamp", "0"))
        
        # Generate unique record hash for deduplication
        record_hash = generate_record_hash(
            tx.get("hash"),
            tx.get("blockNumber"),
            tx.get("from"),
            tx.get("to"),
            value_wei
        )
        
        return {
            "transaction_hash": tx.get("hash", "").lower(),
            "block_number": int(tx.get("blockNumber", 0)),
            "block_hash": tx.get("blockHash", "").lower() if tx.get("blockHash") else None,
            "transaction_timestamp": unix_to_datetime(timestamp).isoformat(),
            "from_address": normalize_address(tx.get("from", "")),
            "to_address": normalize_address(tx.get("to", "")) if tx.get("to") else None,
            "value_wei": value_wei,
            "value_eth": wei_to_ether(value_wei),
            "gas": int(tx.get("gas", 0)) if tx.get("gas") else None,
            "gas_price": int(tx.get("gasPrice", 0)) if tx.get("gasPrice") else None,
            "gas_used": int(tx.get("gasUsed", 0)) if tx.get("gasUsed") else None,
            "nonce": int(tx.get("nonce", 0)) if tx.get("nonce") else None,
            "transaction_index": int(tx.get("transactionIndex", 0)) if tx.get("transactionIndex") else None,
            "input_data": tx.get("input") if tx.get("input") != "0x" else None,
            "contract_address": normalize_address(tx.get("contractAddress", "")) if tx.get("contractAddress") else None,
            "is_error": tx.get("isError") == "1",
            "txreceipt_status": tx.get("txreceipt_status"),
            "confirmations": int(tx.get("confirmations", 0)) if tx.get("confirmations") else None,
            "method_id": tx.get("methodId"),
            "function_name": tx.get("functionName"),
            "record_hash": record_hash,
            "source": f"etherscan:{source_address}",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
    
    def _get_existing_hashes(self, tx_hashes: List[str]) -> set:
        """
        Check which transaction hashes already exist in BigQuery.
        
        Args:
            tx_hashes: List of transaction hashes to check
            
        Returns:
            set: Set of existing transaction hashes
        """
        if not tx_hashes:
            return set()
        
        if not self.bq.table_exists(self.raw_dataset, self.raw_table):
            return set()
        
        # Batch check for existing hashes
        hash_list = ", ".join(f"'{h}'" for h in tx_hashes)
        query = f"""
        SELECT DISTINCT transaction_hash
        FROM `{self.bq.project_id}.{self.raw_dataset}.{self.raw_table}`
        WHERE transaction_hash IN ({hash_list})
        """
        
        results = self.bq.execute_query(query)
        return {row["transaction_hash"] for row in results}
    
    def ingest_address_transactions(
        self,
        address: str,
        start_block: int = 0,
        end_block: int = 99999999,
        include_internal: bool = False
    ) -> int:
        """
        Ingest all transactions for a specific address.
        
        Args:
            address: Ethereum address to ingest transactions for
            start_block: Starting block number
            end_block: Ending block number
            include_internal: Whether to include internal transactions
            
        Returns:
            int: Number of transactions inserted
        """
        address = normalize_address(address)
        self.logger.info(f"Starting ingestion for address {address}")
        
        total_inserted = 0
        page = 1
        
        while True:
            # Fetch transactions from Etherscan
            self.logger.info(f"Fetching page {page} for {address}...")
            
            transactions = self.etherscan.get_transactions_by_address(
                address=address,
                start_block=start_block,
                end_block=end_block,
                page=page,
                offset=self.batch_size
            )
            
            if not transactions:
                self.logger.info(f"No more transactions found for {address}")
                break
            
            self.stats["total_fetched"] += len(transactions)
            
            # Transform transactions
            transformed = [
                self._transform_transaction(tx, address)
                for tx in transactions
            ]
            
            # Check for duplicates
            tx_hashes = [t["transaction_hash"] for t in transformed]
            existing_hashes = self._get_existing_hashes(tx_hashes)
            
            # Filter out duplicates
            new_transactions = [
                t for t in transformed
                if t["transaction_hash"] not in existing_hashes
            ]
            
            duplicates_count = len(transformed) - len(new_transactions)
            self.stats["duplicates_skipped"] += duplicates_count
            
            if duplicates_count > 0:
                self.logger.debug(f"Skipped {duplicates_count} duplicate transactions")
            
            # Insert new transactions
            if new_transactions:
                inserted = self.bq.insert_rows(
                    dataset_id=self.raw_dataset,
                    table_id=self.raw_table,
                    rows=new_transactions,
                    schema=RAW_TRANSACTIONS_SCHEMA
                )
                total_inserted += inserted
                self.stats["total_inserted"] += inserted
            
            # Update checkpoint with latest block
            if transactions:
                max_block = max(int(tx.get("blockNumber", 0)) for tx in transactions)
                self.checkpoint.set_checkpoint(
                    PIPELINE_NAME,
                    f"{CHECKPOINT_KEY_LAST_BLOCK}:{address}",
                    max_block
                )
            
            # Check if we got less than requested (end of data)
            if len(transactions) < self.batch_size:
                break
            
            page += 1
        
        # Optionally ingest internal transactions
        if include_internal:
            internal_count = self._ingest_internal_transactions(
                address, start_block, end_block
            )
            total_inserted += internal_count
        
        self.logger.info(
            f"Completed ingestion for {address}: "
            f"{total_inserted} transactions inserted"
        )
        
        return total_inserted
    
    def _ingest_internal_transactions(
        self,
        address: str,
        start_block: int,
        end_block: int
    ) -> int:
        """
        Ingest internal transactions for an address.
        
        Args:
            address: Ethereum address
            start_block: Starting block
            end_block: Ending block
            
        Returns:
            int: Number of internal transactions inserted
        """
        self.logger.info(f"Fetching internal transactions for {address}...")
        
        internal_txs = self.etherscan.get_internal_transactions(
            address=address,
            start_block=start_block,
            end_block=end_block
        )
        
        if not internal_txs:
            return 0
        
        # Transform internal transactions (similar structure)
        transformed = []
        for tx in internal_txs:
            value_wei = parse_hex_to_int(tx.get("value", "0"))
            timestamp = int(tx.get("timeStamp", "0"))
            
            record_hash = generate_record_hash(
                tx.get("hash"),
                tx.get("blockNumber"),
                tx.get("from"),
                tx.get("to"),
                value_wei,
                "internal"
            )
            
            transformed.append({
                "transaction_hash": tx.get("hash", "").lower(),
                "block_number": int(tx.get("blockNumber", 0)),
                "block_hash": None,
                "transaction_timestamp": unix_to_datetime(timestamp).isoformat(),
                "from_address": normalize_address(tx.get("from", "")),
                "to_address": normalize_address(tx.get("to", "")) if tx.get("to") else None,
                "value_wei": value_wei,
                "value_eth": wei_to_ether(value_wei),
                "gas": int(tx.get("gas", 0)) if tx.get("gas") else None,
                "gas_price": None,
                "gas_used": int(tx.get("gasUsed", 0)) if tx.get("gasUsed") else None,
                "nonce": None,
                "transaction_index": None,
                "input_data": tx.get("input") if tx.get("input") != "0x" else None,
                "contract_address": normalize_address(tx.get("contractAddress", "")) if tx.get("contractAddress") else None,
                "is_error": tx.get("isError") == "1",
                "txreceipt_status": None,
                "confirmations": None,
                "method_id": None,
                "function_name": None,
                "record_hash": record_hash,
                "source": f"etherscan_internal:{address}",
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            })
        
        # Check for duplicates and insert
        tx_hashes = [t["transaction_hash"] for t in transformed]
        existing_hashes = self._get_existing_hashes(tx_hashes)
        new_transactions = [
            t for t in transformed
            if t["transaction_hash"] not in existing_hashes
        ]
        
        if new_transactions:
            return self.bq.insert_rows(
                dataset_id=self.raw_dataset,
                table_id=self.raw_table,
                rows=new_transactions,
                schema=RAW_TRANSACTIONS_SCHEMA
            )
        
        return 0
    
    def run(
        self,
        addresses: List[str],
        start_block: int = None,
        end_block: int = None,
        resume: bool = True,
        include_internal: bool = False
    ) -> Dict:
        """
        Run the ingestion pipeline for multiple addresses.
        
        Args:
            addresses: List of Ethereum addresses to ingest
            start_block: Starting block (None for checkpoint/0)
            end_block: Ending block (None for latest)
            resume: Whether to resume from checkpoints
            include_internal: Whether to include internal transactions
            
        Returns:
            Dict: Ingestion statistics
        """
        self.logger.info(f"Starting transaction ingestion for {len(addresses)} addresses")
        
        # Get latest block if end_block not specified
        if end_block is None:
            end_block = self.etherscan.get_latest_block_number()
            self.logger.info(f"Latest block: {end_block}")
        
        for address in addresses:
            address = normalize_address(address)
            
            # Determine start block
            addr_start_block = start_block
            if addr_start_block is None and resume:
                checkpoint_value = self.checkpoint.get_checkpoint(
                    PIPELINE_NAME,
                    f"{CHECKPOINT_KEY_LAST_BLOCK}:{address}",
                    default="0"
                )
                addr_start_block = int(checkpoint_value) + 1
            elif addr_start_block is None:
                addr_start_block = 0
            
            self.logger.info(
                f"Processing {address} from block {addr_start_block} to {end_block}"
            )
            
            try:
                self.ingest_address_transactions(
                    address=address,
                    start_block=addr_start_block,
                    end_block=end_block,
                    include_internal=include_internal
                )
            except Exception as e:
                self.logger.error(f"Error ingesting {address}: {e}")
                self.stats["errors"] += 1
                continue
        
        self.logger.info(f"Ingestion complete. Stats: {self.stats}")
        return self.stats


def main():
    """Main entry point for CLI execution."""
    parser = argparse.ArgumentParser(
        description="Ingest blockchain transactions from Etherscan to BigQuery"
    )
    parser.add_argument(
        "--addresses",
        nargs="+",
        help="Ethereum addresses to ingest"
    )
    parser.add_argument(
        "--addresses-file",
        type=str,
        help="Path to file containing addresses (one per line)"
    )
    parser.add_argument(
        "--start-block",
        type=int,
        default=None,
        help="Starting block number"
    )
    parser.add_argument(
        "--end-block",
        type=int,
        default=None,
        help="Ending block number"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        default=True,
        help="Resume from checkpoints"
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Don't resume from checkpoints"
    )
    parser.add_argument(
        "--include-internal",
        action="store_true",
        help="Include internal transactions"
    )
    
    args = parser.parse_args()
    
    # Collect addresses
    addresses = []
    if args.addresses:
        addresses.extend(args.addresses)
    if args.addresses_file:
        with open(args.addresses_file, "r") as f:
            addresses.extend([line.strip() for line in f if line.strip()])
    
    if not addresses:
        parser.error("No addresses provided. Use --addresses or --addresses-file")
    
    # Run pipeline
    pipeline = TransactionIngestionPipeline()
    stats = pipeline.run(
        addresses=addresses,
        start_block=args.start_block,
        end_block=args.end_block,
        resume=not args.no_resume,
        include_internal=args.include_internal
    )
    
    print(f"\nIngestion Statistics:")
    print(f"  Total Fetched: {stats['total_fetched']}")
    print(f"  Total Inserted: {stats['total_inserted']}")
    print(f"  Duplicates Skipped: {stats['duplicates_skipped']}")
    print(f"  Errors: {stats['errors']}")


if __name__ == "__main__":
    main()

