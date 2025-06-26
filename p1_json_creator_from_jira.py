#!/usr/bin/env python3
import csv
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Set

# Increase CSV field size limit
csv.field_size_limit(sys.maxsize)

def process_logs():
    base_path = Path("inputs/logs")
    all_entries = []
    seen_keys = set()
    
    required_fields = [
        "Summary", "Issue key", "Issue id", "Issue Type", "Status", 
        "Project name", "Priority", "Resolution", "Created", "Resolved",
        "Affects Version/s", "Fix Version/s", "Attachment"
    ]
    
    for csv_file in base_path.rglob("*.csv"):
        print(f"Processing: {csv_file}")
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            # Read header
            header = next(reader)
            
            # Create index mapping for required fields
            field_indices = {}
            for field in required_fields:
                if field not in ["Affects Version/s", "Fix Version/s", "Attachment"]:
                    # Find first occurrence
                    for i, col in enumerate(header):
                        if col == field:
                            field_indices[field] = i
                            break
            
            # Find all indices for multi-value fields
            affects_version_indices = [i for i, col in enumerate(header) if col == "Affects Version/s"]
            fix_version_indices = [i for i, col in enumerate(header) if col == "Fix Version/s"]
            attachment_indices = [i for i, col in enumerate(header) if col == "Attachment"]
            
            for row in reader:
                if len(row) > field_indices.get("Issue key", 0):
                    issue_key = row[field_indices.get("Issue key", 0)]
                    
                    if issue_key and issue_key not in seen_keys:
                        seen_keys.add(issue_key)
                        
                        entry = {}
                        
                        # Extract single-value fields
                        for field in required_fields:
                            if field not in ["Affects Version/s", "Fix Version/s", "Attachment"]:
                                idx = field_indices.get(field)
                                if idx is not None and idx < len(row):
                                    entry[field] = row[idx]
                                else:
                                    entry[field] = ""
                        
                        # Extract multi-value fields
                        affects_versions = []
                        for idx in affects_version_indices:
                            if idx < len(row) and row[idx] and row[idx].strip():
                                affects_versions.append(row[idx].strip())
                        
                        fix_versions = []
                        for idx in fix_version_indices:
                            if idx < len(row) and row[idx] and row[idx].strip():
                                fix_versions.append(row[idx].strip())
                        
                        attachments = []
                        for idx in attachment_indices:
                            if idx < len(row) and row[idx] and row[idx].strip():
                                # Parse attachment format: "date;username;filename;url"
                                parts = row[idx].strip().split(';')
                                if len(parts) >= 4:
                                    attachment = {
                                        "date": parts[0],
                                        "username": parts[1],
                                        "filename": parts[2],
                                        "url": parts[3]
                                    }
                                    attachments.append(attachment)
                                else:
                                    # Fallback for unexpected format
                                    attachments.append({"raw": row[idx].strip()})
                        
                        entry["Affects Version/s"] = affects_versions
                        entry["Fix Version/s"] = fix_versions
                        entry["Attachment"] = attachments
                        
                        all_entries.append(entry)
    
    print(f"\nTotal unique entries: {len(all_entries)}")
    
    output_file = ("outputs/p1_issues.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_entries, f, indent=2, ensure_ascii=False)
    
    print(f"Results saved to: {output_file}")

if __name__ == "__main__":
    process_logs()