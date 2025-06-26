#!/usr/bin/env python3
"""
CSV processing module for LogFinder.
Handles reading JIRA CSV exports and converting them to structured JSON.
"""
import csv
from pathlib import Path
from typing import Dict, List, Set
from .utils import Config, save_json, print_progress


class CSVProcessor:
    """Process JIRA CSV files into structured JSON format."""
    
    def __init__(self, config: Config):
        self.config = config
        self.required_fields = [
            "Summary", "Issue key", "Issue id", "Issue Type", "Status", 
            "Project name", "Priority", "Resolution", "Created", "Resolved",
            "Affects Version/s", "Fix Version/s", "Attachment"
        ]
    
    def process_all_logs(self) -> Path:
        """Process all CSV files in the logs directory."""
        logs_dir = self.config.inputs_dir / "logs"
        all_entries = []
        seen_keys = set()
        
        csv_files = list(logs_dir.rglob("*.csv"))
        print(f"Found {len(csv_files)} CSV files to process")
        
        for i, csv_file in enumerate(csv_files):
            print_progress(i + 1, len(csv_files), f"Processing: {csv_file.name}")
            entries = self._process_csv_file(csv_file, seen_keys)
            all_entries.extend(entries)
            print(f" -> {len(entries)} new entries")
        
        print(f"\nTotal unique entries: {len(all_entries)}")
        
        # Save results
        output_file = self.config.outputs_dir / "issues.json"
        save_json(all_entries, output_file)
        print(f"Results saved to: {output_file}")
        
        return output_file
    
    def _process_csv_file(self, csv_file: Path, seen_keys: Set[str]) -> List[Dict]:
        """Process a single CSV file."""
        entries = []
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            # Create field mappings
            field_indices = self._create_field_mapping(header)
            
            for row in reader:
                if len(row) > field_indices.get("Issue key", 0):
                    issue_key = row[field_indices.get("Issue key", 0)]
                    
                    if issue_key and issue_key not in seen_keys:
                        seen_keys.add(issue_key)
                        entry = self._extract_entry_data(row, field_indices, header)
                        entries.append(entry)
        
        return entries
    
    def _create_field_mapping(self, header: List[str]) -> Dict[str, int]:
        """Create mapping from field names to column indices."""
        field_indices = {}
        
        # Map single-value fields
        for field in self.required_fields:
            if field not in ["Affects Version/s", "Fix Version/s", "Attachment"]:
                for i, col in enumerate(header):
                    if col == field:
                        field_indices[field] = i
                        break
        
        return field_indices
    
    def _extract_entry_data(self, row: List[str], field_indices: Dict[str, int], header: List[str]) -> Dict:
        """Extract structured data from a CSV row."""
        entry = {}
        
        # Extract single-value fields
        for field in self.required_fields:
            if field not in ["Affects Version/s", "Fix Version/s", "Attachment"]:
                idx = field_indices.get(field)
                entry[field] = row[idx] if idx is not None and idx < len(row) else ""
        
        # Extract multi-value fields
        entry["Affects Version/s"] = self._extract_multi_values(row, header, "Affects Version/s")
        entry["Fix Version/s"] = self._extract_multi_values(row, header, "Fix Version/s")
        entry["Attachment"] = self._extract_attachments(row, header)
        
        return entry
    
    def _extract_multi_values(self, row: List[str], header: List[str], field_name: str) -> List[str]:
        """Extract multiple values for a field (e.g., versions)."""
        values = []
        indices = [i for i, col in enumerate(header) if col == field_name]
        
        for idx in indices:
            if idx < len(row) and row[idx] and row[idx].strip():
                values.append(row[idx].strip())
        
        return values
    
    def _extract_attachments(self, row: List[str], header: List[str]) -> List[Dict]:
        """Extract and parse attachment information."""
        attachments = []
        attachment_indices = [i for i, col in enumerate(header) if col == "Attachment"]
        
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
        
        return attachments