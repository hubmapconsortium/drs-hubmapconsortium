"""
Comprehensive DRS Synchronization Script

This script synchronizes the HubMAP DRS (Data Repository Service) with the Search API and UUID API.

It performs the following operations:
1. Fetches all published primary/component datasets from Search API
2. Fetches all files for those datasets from UUID API
3. Fetches current datasets and files from DRS
4. Compares to identify missing datasets and files
5. Generates CSV files for import (manifest.csv and files.csv)
6. Outputs SQL queries for database updates

Requirements:
- pandas
- requests
"""

import pandas as pd
import requests
from typing import List, Dict
from datetime import datetime
import time
import json
import sys
import os
import argparse

# Add parent directory to path to import from app.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app import execute_sql_query, connect_to_database

# Load configuration from JSON file
config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
with open(config_path) as config_file:
    config_data = json.load(config_file)

# Configuration
BEARER_TOKEN = config_data.get("BEARER_TOKEN", "")
SEARCH_API_URL = "https://search.api.hubmapconsortium.org/v3/portal/search"
UUID_API_URL = "https://uuid.api.hubmapconsortium.org"

class DRSSynchronizer:
    def __init__(self, bearer_token: str = None):
        """Initialize synchronizer with optional bearer token.

        Args:
            bearer_token: Optional bearer token for API authentication.
                         If not provided, uses token from config.
        """
        self.bearer_token = bearer_token or BEARER_TOKEN
        self.headers = {'Authorization': f'Bearer {self.bearer_token}'}

    # ============================================================================
    # STEP 1: GATHER DATA FROM SEARCH API
    # ============================================================================

    def get_published_datasets_from_search_api(self) -> pd.DataFrame:
        """
        Fetch all published primary and component datasets from Search API.

        Returns:
            DataFrame with dataset information including uuid, doi_url, group_name, etc.
        """
        print("\n[1/6] Fetching published datasets from Search API...")

        body = {
            "_source": [
                "entity_type", "creation_action", "dataset_type", "dbgap_study_url",
                "doi_url", "donor.hubmap_id", "immediate_ancestor_ids", "mapped_data_types",
                "origin_samples_unique_mapped_organs", "status", "group_name",
                "published_timestamp", "hubmap_id", "title", "uuid"
            ],
            "size": 10000,  # Adjust if you have more than 10k datasets
            "query": {
                "bool": {
                    "should": [
                        {"match": {"creation_action": "Create Dataset Activity"}},  # Primary datasets
                    ],
                    "must": [
                        {"match": {"entity_type": "Dataset"}},
                        {"match": {"status": "Published"}}
                    ],
                    "minimum_should_match": 1
                }
            }
        }

        try:
            response = requests.post(url=SEARCH_API_URL, headers=self.headers, json=body, timeout=60)
            response.raise_for_status()
            data = response.json()

            df = pd.json_normalize(data, record_path=['hits', 'hits'])

            # Rename columns for clarity
            df = df.rename(columns={
                '_id': 'uuid',
                '_source.hubmap_id': 'hubmap_id',
                '_source.dataset_type': 'dataset_type',
                '_source.doi_url': 'doi_url',
                '_source.group_name': 'group_name',
                '_source.published_timestamp': 'published_timestamp',
                '_source.dbgap_study_url': 'dbgap_study_url'
            })

            # Keep only relevant columns
            columns_to_keep = ['uuid', 'hubmap_id', 'dataset_type', 'doi_url',
                             'group_name', 'published_timestamp', 'dbgap_study_url']
            df = df[[col for col in columns_to_keep if col in df.columns]]

            print(f"   Found {len(df)} published datasets in Search API")
            return df

        except requests.exceptions.RequestException as e:
            print(f"   Error fetching from Search API: {e}")
            return pd.DataFrame()

    # ============================================================================
    # STEP 2: GATHER DATA FROM UUID API
    # ============================================================================

    def get_files_from_uuid_api(self, dataset_uuids: List[str]) -> pd.DataFrame:
        """
        Fetch file information from UUID API for given datasets.

        Args:
            dataset_uuids: List of dataset UUIDs to fetch files for

        Returns:
            DataFrame with file information including file_uuid, checksum, size, etc.
        """
        print("\n[2/6] Fetching files from UUID API...")
        print(f"   Processing {len(dataset_uuids)} datasets...")

        all_files = []
        errors = []

        for idx, dataset_uuid in enumerate(dataset_uuids, 1):
            if idx % 100 == 0:
                print(f"   Progress: {idx}/{len(dataset_uuids)} datasets processed")

            url = f"{UUID_API_URL}/{dataset_uuid}/files"
            try:
                response = requests.get(url=url, headers=self.headers, timeout=30)
                if response.status_code == 303:
                    redirect_url = response.text
                    if redirect_url:
                        print(f"Following 303 redirect to: {redirect_url}")
                        response = requests.get(url=redirect_url, headers=self.headers)
                    else:
                        print(f"303 redirect received but no Location header for dataset+ {dataset_uuid}")
                        continue

                if response.status_code == 200:
                    files_data = response.json()
                    for file_info in files_data:
                        file_info['dataset_uuid'] = dataset_uuid
                        all_files.append(file_info)
                elif response.status_code == 404:
                    # Dataset exists in Search API but has no files or doesn't exist in UUID API
                    continue
                else:
                    errors.append(f"Dataset {dataset_uuid}: HTTP {response.status_code}")
            except requests.exceptions.RequestException as e:
                errors.append(f"Dataset {dataset_uuid}: {str(e)}")
                continue

        if errors:
            print(f"   Encountered {len(errors)} errors (saved to uuid_api_errors.log)")
            with open('uuid_api_errors.log', 'w') as f:
                f.write('\n'.join(errors))

        df = pd.DataFrame(all_files)
        df.rename(columns={"file_uuid": "uuid"}, inplace=True)
        print(f"   Found {len(df)} files in UUID API")
        return df

    # ============================================================================
    # STEP 3: GATHER DATA FROM DRS
    # ============================================================================

    def get_datasets_from_drs(self) -> pd.DataFrame:
        """
        Fetch all datasets currently in DRS database.

        Returns:
            DataFrame with DRS dataset information
        """
        print("\n[3/6] Fetching datasets from DRS database...")

        try:
            query = "SELECT uuid, hubmap_id FROM manifest"
            datasets = execute_sql_query(query)
            df = pd.DataFrame(datasets)
            print(f"   Found {len(df)} datasets in DRS")
            return df
        except Exception as e:
            print(f"   Error fetching from DRS database: {e}")
            return pd.DataFrame()

    def get_files_from_drs(self) -> pd.DataFrame:
        """
        Fetch file information from DRS database.

        Returns:
            DataFrame with file information from DRS
        """
        print("\n[4/6] Fetching files from DRS database...")

        try:
            query = "SELECT hubmap_id, file_uuid as file_id, name as file_name FROM files"
            file_records = execute_sql_query(query)
            df = pd.DataFrame(file_records)
            print(f"   Found {len(df)} files in DRS")
            return df
        except Exception as e:
            print(f"   Error fetching from DRS database: {e}")
            return pd.DataFrame()

    # ============================================================================
    # STEP 5: COMPARISONS
    # ============================================================================

    def compare_and_identify_missing(
        self,
        search_datasets: pd.DataFrame,
        uuid_files: pd.DataFrame,
        drs_datasets: pd.DataFrame,
        drs_files: pd.DataFrame
    ) -> Dict[str, pd.DataFrame]:
        """
        Compare data sources and identify datasets/files to add and delete.

        Returns:
            Dict with keys: 'datasets_to_add', 'files_to_add',
                           'datasets_to_delete', 'files_to_delete'
        """
        print("\n[5/6] Performing comparisons...")

        # Datasets in Search API but not in DRS (TO ADD)
        search_uuids = set(search_datasets['uuid'].values) if 'uuid' in search_datasets.columns else set()
        drs_uuids = set(drs_datasets['uuid'].values) if 'uuid' in drs_datasets.columns else set()
        missing_dataset_uuids = search_uuids - drs_uuids

        datasets_to_add = search_datasets[search_datasets['uuid'].isin(missing_dataset_uuids)]
        print(f"   Datasets to ADD (in Search API but not in DRS): {len(datasets_to_add)}")

        # Datasets in DRS but not in Search API (TO DELETE)
        extra_dataset_uuids = drs_uuids - search_uuids
        datasets_to_delete = drs_datasets[drs_datasets['uuid'].isin(extra_dataset_uuids)]
        print(f"   Datasets to DELETE (in DRS but not in Search API): {len(datasets_to_delete)}")

        # Files in UUID API but not in DRS (TO ADD)
        uuid_file_ids = set(uuid_files['uuid'].values) if 'uuid' in uuid_files.columns else set()
        drs_file_ids = set(drs_files['file_id'].values) if 'file_id' in drs_files.columns else set()
        missing_file_ids = uuid_file_ids - drs_file_ids

        files_to_add = uuid_files[uuid_files['uuid'].isin(missing_file_ids)]
        print(f"   Files to ADD (in UUID API but not in DRS): {len(files_to_add)}")

        # Files in DRS but not in UUID API (TO DELETE)
        extra_file_ids = drs_file_ids - uuid_file_ids
        files_to_delete = drs_files[drs_files['file_id'].isin(extra_file_ids)]
        print(f"   Files to DELETE (in DRS but not in UUID API): {len(files_to_delete)}")

        return {
            'datasets_to_add': datasets_to_add,
            'files_to_add': files_to_add,
            'datasets_to_delete': datasets_to_delete,
            'files_to_delete': files_to_delete
        }

    # ============================================================================
    # STEP 6: GENERATE CSV FILES FOR IMPORT
    # ============================================================================

    def generate_manifest_csv(
        self,
        missing_datasets: pd.DataFrame,
        uuid_files: pd.DataFrame,
        output_file: str = "manifest.csv"
    ) -> None:
        """
        Generate manifest.csv for importing new datasets into DRS.

        CSV columns: uuid, hubmap_id, creation_date, dataset_type, directory,
                    doi_url, group_name, is_protected, number_of_files, pretty_size
        """
        print("\n[6/6] Generating manifest.csv...")

        if missing_datasets.empty:
            print("   No missing datasets to export")
            return

        manifest_data = []

        for _, dataset in missing_datasets.iterrows():
            uuid = dataset.get('uuid', '')
            hubmap_id = dataset.get('hubmap_id', '')

            # Get files for this dataset
            dataset_files = uuid_files[uuid_files['dataset_uuid'] == uuid] if 'dataset_uuid' in uuid_files.columns else pd.DataFrame()

            # Calculate number of files and total size
            number_of_files = len(dataset_files)
            total_size = dataset_files['size'].sum() if 'size' in dataset_files.columns else 0

            # Calculate pretty size
            if total_size >= 1099511627776:  # >= 1TB
                pretty_size = f"{round(total_size / 1099511627776, 1)}T"
            elif total_size >= 1073741824:  # >= 1GB
                pretty_size = f"{round(total_size / 1073741824, 1)}G"
            elif total_size >= 1048576:  # >= 1MB
                pretty_size = f"{round(total_size / 1048576, 1)}M"
            elif total_size >= 1024:  # >= 1KB
                pretty_size = f"{round(total_size / 1024, 1)}K"
            else:
                pretty_size = f"{total_size}B"

            manifest_data.append({
                'uuid': uuid,
                'hubmap_id': hubmap_id,
                'creation_date': dataset.get('published_timestamp', datetime.now().isoformat()),
                'dataset_type': dataset.get('dataset_type', ''),
                'directory': '',  # This would need to be populated from file system info
                'doi_url': dataset.get('doi_url', ''),
                'group_name': dataset.get('group_name', ''),
                'is_protected': 1 if 'dbgap' in str(dataset.get('dbgap_study_url', '')).lower() else 0,
                'number_of_files': number_of_files,
                'pretty_size': pretty_size
            })

        manifest_df = pd.DataFrame(manifest_data)
        manifest_df.to_csv(output_file, index=False)
        print(f"   Generated {output_file} with {len(manifest_df)} entries")

    def generate_files_csv(
        self,
        missing_files: pd.DataFrame,
        output_file: str = "files.csv"
    ) -> None:
        """
        Generate files.csv for importing new files into DRS.

        CSV columns: hubmap_id, drs_uri, name, dbgap_study_id, file_uuid, checksum, size
        """
        print(f"\nGenerating {output_file}...")

        if missing_files.empty:
            print("   No missing files to export")
            return

        files_data = []

        for _, file_info in missing_files.iterrows():
            file_uuid = file_info.get('uuid', '')

            files_data.append({
                'hubmap_id': file_info.get('dataset_uuid', ''),
                'drs_uri': f"drs://drs.hubmapconsortium.org/{file_uuid}",
                'name': file_info.get('filename', file_info.get('path', '')),
                'dbgap_study_id': '',  # Populate if available
                'file_uuid': file_uuid,
                'checksum': file_info.get('checksum', ''),
                'size': file_info.get('size', 0)
            })

        files_df = pd.DataFrame(files_data)
        files_df.to_csv(output_file, index=False)
        print(f"   Generated {output_file} with {len(files_df)} entries")

    def generate_datasets_to_delete_csv(
        self,
        datasets_to_delete: pd.DataFrame,
        output_file: str = "datasets_to_delete.csv"
    ) -> None:
        """
        Generate CSV file listing datasets to delete from DRS.

        CSV columns: uuid, hubmap_id
        """
        print(f"\nGenerating {output_file}...")

        if datasets_to_delete.empty:
            print("   No datasets to delete")
            return

        datasets_to_delete.to_csv(output_file, index=False)
        print(f"   Generated {output_file} with {len(datasets_to_delete)} entries")

    def generate_files_to_delete_csv(
        self,
        files_to_delete: pd.DataFrame,
        output_file: str = "files_to_delete.csv"
    ) -> None:
        """
        Generate CSV file listing files to delete from DRS.

        CSV columns: file_id, hubmap_id, file_name
        """
        print(f"\nGenerating {output_file}...")

        if files_to_delete.empty:
            print("   No files to delete")
            return

        files_to_delete.to_csv(output_file, index=False)
        print(f"   Generated {output_file} with {len(files_to_delete)} entries")

    # ============================================================================
    # STEP 7: EXECUTE SYNC OPERATIONS
    # ============================================================================

    def execute_sync_operations(self, comparison: Dict[str, pd.DataFrame], uuid_files: pd.DataFrame) -> None:
        """
        Execute the actual database operations to sync DRS with upstream APIs.

        Args:
            comparison: Dict with datasets_to_add, files_to_add, datasets_to_delete, files_to_delete
            uuid_files: DataFrame with all files from UUID API (for calculating sizes)
        """
        print("\n" + "=" * 80)
        print("EXECUTING SYNCHRONIZATION OPERATIONS")
        print("=" * 80)

        conn = connect_to_database()
        cursor = conn.cursor()

        try:
            # Step 1: Insert new datasets
            datasets_to_add = comparison['datasets_to_add']
            if not datasets_to_add.empty:
                print(f"\n[1/5] Inserting {len(datasets_to_add)} new datasets...")
                for _, dataset in datasets_to_add.iterrows():
                    uuid = dataset.get('uuid', '')
                    hubmap_id = dataset.get('hubmap_id', '')

                    # Get files for this dataset to calculate size
                    dataset_files = uuid_files[uuid_files['dataset_uuid'] == uuid] if 'dataset_uuid' in uuid_files.columns else pd.DataFrame()
                    number_of_files = len(dataset_files)
                    total_size = int(dataset_files['size'].sum()) if 'size' in dataset_files.columns else 0

                    # Calculate pretty size
                    if total_size >= 1099511627776:
                        pretty_size = f"{round(total_size / 1099511627776, 1)}T"
                    elif total_size >= 1073741824:
                        pretty_size = f"{round(total_size / 1073741824, 1)}G"
                    elif total_size >= 1048576:
                        pretty_size = f"{round(total_size / 1048576, 1)}M"
                    elif total_size >= 1024:
                        pretty_size = f"{round(total_size / 1024, 1)}K"
                    else:
                        pretty_size = f"{total_size}B"

                    insert_query = """
                        INSERT INTO manifest (uuid, hubmap_id, creation_date, dataset_type, directory,
                                            doi_url, group_name, is_protected, number_of_files, pretty_size)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        uuid,
                        hubmap_id,
                        dataset.get('published_timestamp', datetime.now().isoformat()),
                        dataset.get('dataset_type', ''),
                        '',  # directory
                        dataset.get('doi_url', ''),
                        dataset.get('group_name', ''),
                        1 if 'dbgap' in str(dataset.get('dbgap_study_url', '')).lower() else 0,
                        number_of_files,
                        pretty_size
                    ))
                conn.commit()
                print(f"   ✓ Inserted {len(datasets_to_add)} datasets")
            else:
                print("\n[1/5] No new datasets to insert")

            # Step 2: Insert new files
            files_to_add = comparison['files_to_add']
            if not files_to_add.empty:
                print(f"\n[2/5] Inserting {len(files_to_add)} new files...")
                for _, file_info in files_to_add.iterrows():
                    file_uuid = file_info.get('uuid', '')
                    insert_query = """
                        INSERT INTO files (hubmap_id, drs_uri, name, dbgap_study_id, file_uuid, checksum, size)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        file_info.get('dataset_uuid', ''),
                        f"drs://drs.hubmapconsortium.org/{file_uuid}",
                        file_info.get('filename', file_info.get('path', '')),
                        '',  # dbgap_study_id
                        file_uuid,
                        file_info.get('checksum', ''),
                        int(file_info.get('size', 0))
                    ))
                conn.commit()
                print(f"   ✓ Inserted {len(files_to_add)} files")
            else:
                print("\n[2/5] No new files to insert")

            # Step 3: Delete files that are no longer in UUID API
            files_to_delete = comparison['files_to_delete']
            if not files_to_delete.empty:
                print(f"\n[3/5] Deleting {len(files_to_delete)} files...")
                for _, file_info in files_to_delete.iterrows():
                    delete_query = "DELETE FROM files WHERE file_uuid = %s"
                    cursor.execute(delete_query, (file_info.get('file_id', ''),))
                conn.commit()
                print(f"   ✓ Deleted {len(files_to_delete)} files")
            else:
                print("\n[3/5] No files to delete")

            # Step 4: Delete datasets that are no longer in Search API
            datasets_to_delete = comparison['datasets_to_delete']
            if not datasets_to_delete.empty:
                print(f"\n[4/5] Deleting {len(datasets_to_delete)} datasets...")
                for _, dataset in datasets_to_delete.iterrows():
                    # Delete associated files first
                    cursor.execute("DELETE FROM files WHERE hubmap_id = %s", (dataset.get('hubmap_id', ''),))
                    # Delete dataset
                    cursor.execute("DELETE FROM manifest WHERE uuid = %s", (dataset.get('uuid', ''),))
                conn.commit()
                print(f"   ✓ Deleted {len(datasets_to_delete)} datasets (and their associated files)")
            else:
                print("\n[4/5] No datasets to delete")

            # Step 5: Update manifest with recalculated pretty_size and number_of_files
            print("\n[5/5] Updating manifest with file counts and sizes...")

            # Update pretty_size
            update_size_query = """
                UPDATE manifest m
                JOIN (
                    SELECT hubmap_id,
                        CASE
                            WHEN SUM(size) >= 1099511627776 THEN CONCAT(ROUND(SUM(size) / 1099511627776, 1), 'T')
                            WHEN SUM(size) >= 1073741824 THEN CONCAT(ROUND(SUM(size) / 1073741824, 1), 'G')
                            WHEN SUM(size) >= 1048576 THEN CONCAT(ROUND(SUM(size) / 1048576, 1), 'M')
                            ELSE CONCAT(ROUND(SUM(size) / 1024, 1), 'K')
                        END AS pretty_size
                    FROM files
                    GROUP BY hubmap_id
                ) f ON m.hubmap_id = f.hubmap_id
                SET m.pretty_size = f.pretty_size
            """
            cursor.execute(update_size_query)

            # Update number_of_files
            update_count_query = """
                UPDATE manifest m
                JOIN (
                    SELECT hubmap_id, COUNT(*) as number_of_files
                    FROM files
                    GROUP BY hubmap_id
                ) f ON m.hubmap_id = f.hubmap_id
                SET m.number_of_files = f.number_of_files
            """
            cursor.execute(update_count_query)
            conn.commit()
            print("   ✓ Updated manifest metadata")

            print("\n" + "=" * 80)
            print("SYNC OPERATIONS COMPLETED SUCCESSFULLY")
            print("=" * 80)

        except Exception as e:
            conn.rollback()
            print(f"\n✗ ERROR during sync operations: {e}")
            print("   Changes have been rolled back.")
            raise
        finally:
            cursor.close()
            conn.close()

    # ============================================================================
    # MAIN EXECUTION
    # ============================================================================

    def run_sync(self, execute: bool = False) -> None:
        """
        Execute the complete synchronization workflow.

        Args:
            execute: If True, execute database operations. If False, only generate CSVs and print SQL.
        """
        print("=" * 80)
        print("DRS SYNCHRONIZATION SCRIPT")
        print(f"Mode: {'EXECUTE' if execute else 'DRY RUN'}")
        print("=" * 80)
        start_time = time.time()

        # Step 1: Get datasets from Search API
        search_datasets = self.get_published_datasets_from_search_api()

        if search_datasets.empty:
            print("\nERROR: Failed to fetch datasets from Search API. Aborting.")
            return

        # Step 2: Get files from UUID API
        dataset_uuids = search_datasets['uuid'].tolist()
        uuid_files = self.get_files_from_uuid_api(dataset_uuids)

        # Step 3: Get datasets from DRS
        drs_datasets = self.get_datasets_from_drs()

        # Step 4: Get files from DRS
        drs_files = self.get_files_from_drs() if not drs_datasets.empty else pd.DataFrame()

        # Step 5: Compare and identify changes
        comparison = self.compare_and_identify_missing(
            search_datasets, uuid_files, drs_datasets, drs_files
        )

        # Step 6: Generate CSV files
        self.generate_manifest_csv(comparison['datasets_to_add'], uuid_files)
        self.generate_files_csv(comparison['files_to_add'])
        self.generate_datasets_to_delete_csv(comparison['datasets_to_delete'])
        self.generate_files_to_delete_csv(comparison['files_to_delete'])

        # Save intermediate results for analysis
        search_datasets.to_csv('search_api_datasets.csv', index=False)
        uuid_files.to_csv('uuid_api_files.csv', index=False)
        drs_datasets.to_csv('drs_datasets.csv', index=False)
        drs_files.to_csv('drs_files.csv', index=False)

        # Step 7: Execute or dry-run
        if execute:
            # Execute actual database operations
            self.execute_sync_operations(comparison, uuid_files)
        else:
            # Dry run mode: just print SQL instructions
            self.print_sql_instructions()

        elapsed = time.time() - start_time
        print("\n" + "=" * 80)
        print("SYNCHRONIZATION COMPLETE")
        print("=" * 80)
        print(f"Total execution time: {elapsed:.2f} seconds")
        print("\nGenerated files:")
        print("  - manifest.csv (new datasets to import)")
        print("  - files.csv (new files to import)")
        print("  - datasets_to_delete.csv (datasets to remove)")
        print("  - files_to_delete.csv (files to remove)")
        print("\nIntermediate data files:")
        print("  - search_api_datasets.csv")
        print("  - uuid_api_files.csv")
        print("  - drs_datasets.csv")
        print("  - drs_files.csv")

        if not execute:
            print("\nℹ️  This was a DRY RUN. No changes were made to the database.")
            print("   To execute these changes, run the script with --execute flag.")

    def print_sql_instructions(self) -> None:
        """Print SQL commands for importing data and updating the database."""
        print("\n" + "=" * 80)
        print("SQL IMPORT AND UPDATE INSTRUCTIONS")
        print("=" * 80)

        print("\n1. Import manifest entries:")
        print("-" * 80)
        print("""
# First, import the CSV into a temporary table:
mysqlimport --ignore-lines=1 --fields-terminated-by=, --local -u drs_admin -p \\
  --columns=uuid,hubmap_id,creation_date,dataset_type,directory,doi_url,group_name,is_protected,number_of_files,pretty_size \\
  drs manifest_copy.csv

# Then insert only new entries:
INSERT INTO manifest (uuid, hubmap_id, creation_date, dataset_type, directory, doi_url, group_name, is_protected, number_of_files, pretty_size)
SELECT mc.uuid, mc.hubmap_id, mc.creation_date, mc.dataset_type, mc.directory, mc.doi_url, mc.group_name, mc.is_protected, mc.number_of_files, mc.pretty_size
FROM manifest_copy as mc
LEFT JOIN manifest ON mc.uuid = manifest.uuid
WHERE manifest.uuid IS NULL;
        """)

        print("\n2. Import file entries:")
        print("-" * 80)
        print("""
# Import the CSV into a temporary table:
mysqlimport --ignore-lines=1 --fields-terminated-by=, --local -u drs_admin -p \\
  --columns=hubmap_id,drs_uri,name,dbgap_study_id,file_uuid,checksum,size \\
  drs files_copy.csv

# Insert all new file entries:
INSERT INTO files (hubmap_id, drs_uri, name, dbgap_study_id, file_uuid, checksum, size)
SELECT hubmap_id, drs_uri, name, dbgap_study_id, file_uuid, checksum, size
FROM files_copy;
        """)

        print("\n3. Update manifest with pretty_size:")
        print("-" * 80)
        print("""
UPDATE manifest m
JOIN (
  SELECT hubmap_id,
    CASE
      WHEN SUM(size) >= 1099511627776 THEN CONCAT(ROUND(SUM(size) / 1099511627776, 1), 'T')
      WHEN SUM(size) >= 1073741824 THEN CONCAT(ROUND(SUM(size) / 1073741824, 1), 'G')
      WHEN SUM(size) >= 1048576 THEN CONCAT(ROUND(SUM(size) / 1048576, 1), 'M')
      ELSE CONCAT(ROUND(SUM(size) / 1024, 1), 'K')
    END AS pretty_size
  FROM files
  GROUP BY hubmap_id
) f ON m.hubmap_id = f.hubmap_id
SET m.pretty_size = f.pretty_size;
        """)

        print("\n4. Update manifest with number_of_files:")
        print("-" * 80)
        print("""
UPDATE manifest m
JOIN (
  SELECT hubmap_id, COUNT(*) as number_of_files
  FROM files
  GROUP BY hubmap_id
) f ON m.hubmap_id = f.hubmap_id
SET m.number_of_files = f.number_of_files;
        """)

        print("\n5. Delete files that are no longer in UUID API:")
        print("-" * 80)
        print("""
# Import the files_to_delete CSV into a temporary table:
CREATE TEMPORARY TABLE files_to_delete_temp (
  file_id VARCHAR(255),
  hubmap_id VARCHAR(255),
  file_name TEXT
);

LOAD DATA LOCAL INFILE 'files_to_delete.csv'
INTO TABLE files_to_delete_temp
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\\n'
IGNORE 1 ROWS;

# Delete files based on the temporary table:
DELETE files FROM files
INNER JOIN files_to_delete_temp ON files.file_uuid = files_to_delete_temp.file_id;

DROP TEMPORARY TABLE files_to_delete_temp;
        """)

        print("\n6. Delete datasets that are no longer in Search API:")
        print("-" * 80)
        print("""
# Import the datasets_to_delete CSV into a temporary table:
CREATE TEMPORARY TABLE datasets_to_delete_temp (
  uuid VARCHAR(255),
  hubmap_id VARCHAR(255)
);

LOAD DATA LOCAL INFILE 'datasets_to_delete.csv'
INTO TABLE datasets_to_delete_temp
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\\n'
IGNORE 1 ROWS;

# Delete associated files first (foreign key constraint):
DELETE files FROM files
INNER JOIN datasets_to_delete_temp ON files.hubmap_id = datasets_to_delete_temp.hubmap_id;

# Delete datasets based on the temporary table:
DELETE manifest FROM manifest
INNER JOIN datasets_to_delete_temp ON manifest.uuid = datasets_to_delete_temp.uuid;

DROP TEMPORARY TABLE datasets_to_delete_temp;
        """)

        print("\nNote: Always backup your database before running DELETE operations!")
        print("\n" + "=" * 80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Synchronize DRS database with HuBMAP Search API and UUID API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run (default) - analyze changes without modifying database:
  python sync_drs_comprehensive.py

  # Execute changes - actually modify the database:
  python sync_drs_comprehensive.py --execute
        """
    )

    parser.add_argument(
        '--execute',
        action='store_true',
        help='Execute database operations. Without this flag, script runs in dry-run mode (only generates CSVs and prints SQL).'
    )

    args = parser.parse_args()

    synchronizer = DRSSynchronizer(BEARER_TOKEN)
    synchronizer.run_sync(execute=args.execute)


if __name__ == "__main__":
    main()
