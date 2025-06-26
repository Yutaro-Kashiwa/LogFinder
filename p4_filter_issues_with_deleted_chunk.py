#!/usr/bin/env python3
import json
import sys
from pathlib import Path

from src.statistics import analyze_issues_by_project


def has_deleted_chunks(commit):
    """Check if a commit contains only ADDED chunks (no DELETE or other change types)"""
    for file in commit.get('files_changed', {}).get('files', []):
        if not file.get('change_type') == 'MODIFY':
            continue
        if not file.get('path').endswith(('.java')):
            continue
        for chunk in file.get('chunks', []):
            for change in chunk.get('changes', []):
                if change['type']  == 'DELETE':
                    return True
    return False

def filter_issues_with_only_added_chunks(input_file, output_file):
    """Filter issues that have commits with only ADDED chunks"""
    
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    filtered_data = {}
    total_issues = 0
    filtered_issues = 0
    
    for project, issues in data.items():
        filtered_project_issues = {}
        
        for issue_key, issue_data in issues.items():
            total_issues += 1

            has_deleted_chunk = False
            for commit in issue_data.get('commits', []):
                if has_deleted_chunks(commit):
                    has_deleted_chunk = True
                    break
            
            if has_deleted_chunk and issue_data.get('commits'):
                filtered_project_issues[issue_key] = issue_data
                filtered_issues += 1
        
        if filtered_project_issues:
            filtered_data[project] = filtered_project_issues
    
    # Save filtered data
    with open(output_file, 'w') as f:
        json.dump(filtered_data, f, indent=2)
    
    print(f"Total issues processed: {total_issues}")
    print(f"Issues with Deleted chunks: {filtered_issues}")
    print(f"Filtered data saved to: {output_file}")

if __name__ == "__main__":
    input_file = Path("outputs/p3_issues_with_fix_commits.json")
    output_file = Path("outputs/p4_issues_with_deleted_chunks.json")
    
    if not input_file.exists():
        print(f"Error: Input file {input_file} not found!")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_file.parent.mkdir(exist_ok=True)
    
    filter_issues_with_only_added_chunks(input_file, output_file)
