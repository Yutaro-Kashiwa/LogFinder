#!/usr/bin/env python3
"""
Standalone script for searching fix commits using the CommitFinder class.
This replaces the original p3_search_fix_commits.py with a much simpler implementation.
"""
from pathlib import Path
from src.commit_finder import CommitFinder
from src.utils import Config


def search_fix_commits():
    """Search for fix commits using the CommitFinder class."""
    try:
        config = Config()
        finder = CommitFinder(config)
        
        # Use the input file path compatible with the existing workflow
        input_file = Path("outputs/p2_issues_with_logs.json")
        
        output_file = finder.search_fix_commits(input_file)
        print(f"\nCommit search completed successfully.")
        
        return output_file
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please run p2_find_log_attachments.py first.")
    except Exception as e:
        print(f"Error during commit search: {e}")


if __name__ == "__main__":
    search_fix_commits()