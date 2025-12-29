#!/usr/bin/env python3
"""
Main entry point for running Ontario Health data ingestion.

Usage:
    python run_ingestion.py                  # Run all ingestors
    python run_ingestion.py school_cases     # Run specific ingestor
    python run_ingestion.py outbreaks
    python run_ingestion.py --explore        # Explore available datasets
"""
import argparse
import sys
from datetime import datetime


def run_school_cases():
    """Run school cases ingestion."""
    from ingest_school_cases import SchoolCasesIngestor
    
    print("\n" + "="*60)
    print("Running School Cases Ingestion")
    print("="*60)
    
    ingestor = SchoolCasesIngestor()
    return ingestor.run()


def run_outbreaks():
    """Run outbreaks ingestion."""
    from ingest_outbreaks import OutbreaksIngestor
    
    print("\n" + "="*60)
    print("Running Outbreaks Ingestion (Schools/Daycares)")
    print("="*60)
    
    ingestor = OutbreaksIngestor(filter_to_schools=True)
    return ingestor.run()


def run_wastewater():
    """Run wastewater surveillance ingestion (Fall 2025 data)."""
    from ingest_wastewater import WastewaterIngestor
    
    print("\n" + "="*60)
    print("Running Wastewater Surveillance Ingestion (Ontario 2025)")
    print("="*60)
    
    ingestor = WastewaterIngestor(province_filter="Ontario")
    return ingestor.run()


def run_ed_wait_times():
    """Run ED wait times scraper (Halton Healthcare)."""
    from ingest_ed_wait_times import EDWaitTimesIngestor
    
    print("\n" + "="*60)
    print("Running ED Wait Times Ingestion (Halton Healthcare)")
    print("="*60)
    
    ingestor = EDWaitTimesIngestor()
    return ingestor.run()


def explore_datasets():
    """Explore available CKAN datasets."""
    from base_ingestor import CKANDatasetExplorer
    
    print("\n" + "="*60)
    print("Exploring Ontario Data Catalogue")
    print("="*60)
    
    explorer = CKANDatasetExplorer()
    
    queries = [
        "school cases",
        "outbreak",
        "respiratory",
        "influenza children"
    ]
    
    for query in queries:
        print(f"\n--- Searching for: '{query}' ---")
        results = explorer.search_datasets(query, rows=5)
        
        for r in results:
            print(f"\n  {r['title']}")
            print(f"  Slug: {r['name']}")
            for res in r['resources'][:3]:  # Show first 3 resources
                print(f"    - {res['format']}: {res['id'][:20]}...")


def main():
    parser = argparse.ArgumentParser(
        description="Ontario Health Data Pipeline - Ingest public health data into Snowflake"
    )
    parser.add_argument(
        "dataset",
        nargs="?",
        choices=["school_cases", "outbreaks", "wastewater", "ed_wait_times", "all"],
        default="all",
        help="Which dataset to ingest (default: all)"
    )
    parser.add_argument(
        "--explore",
        action="store_true",
        help="Explore available datasets instead of ingesting"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but don't load to Snowflake"
    )
    
    args = parser.parse_args()
    
    print(f"\nOntario Health Data Pipeline")
    print(f"Started at: {datetime.utcnow().isoformat()}Z")
    
    if args.explore:
        explore_datasets()
        return 0
    
    results = []
    
    try:
        if args.dataset in ["school_cases", "all"]:
            results.append(("school_cases", run_school_cases()))
        
        if args.dataset in ["outbreaks", "all"]:
            results.append(("outbreaks", run_outbreaks()))
        
        if args.dataset in ["wastewater", "all"]:
            results.append(("wastewater", run_wastewater()))
        
        if args.dataset in ["ed_wait_times", "all"]:
            results.append(("ed_wait_times", run_ed_wait_times()))
        
    except Exception as e:
        print(f"\nError during ingestion: {e}")
        return 1
    
    # Summary
    print("\n" + "="*60)
    print("INGESTION SUMMARY")
    print("="*60)
    
    all_success = True
    for name, result in results:
        status_emoji = "✓" if result["status"] == "SUCCESS" else "✗"
        print(f"\n{status_emoji} {name}:")
        print(f"    Status: {result['status']}")
        print(f"    Fetched: {result['records_fetched']}")
        print(f"    Inserted: {result['records_inserted']}")
        if result.get("error"):
            print(f"    Note: {result['error']}")
        
        if result["status"] != "SUCCESS":
            all_success = False
    
    print(f"\nCompleted at: {datetime.utcnow().isoformat()}Z")
    
    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())

