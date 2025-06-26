#!/usr/bin/env python3
"""
Log attachment filtering module for LogFinder.
Filters issues that have log file attachments.
"""
from pathlib import Path
from typing import Dict, List
from collections import defaultdict
from .utils import Config, load_json, save_json


class LogFilter:
    """Filter issues that contain log file attachments."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def filter_issues_with_logs(self, input_file: Path = None) -> Path:
        """Filter issues that have attachments with 'log' in the filename."""
        if input_file is None:
            input_file = self.config.outputs_dir / "issues.json"
        
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        print(f"Reading from: {input_file}")
        issues = load_json(input_file)
        
        # Filter issues with log attachments
        issues_with_logs = []
        
        for issue in issues:
            if issue.get("Attachment"):
                log_attachments = self._find_log_attachments(issue["Attachment"])
                
                if log_attachments:
                    filtered_issue = self._create_filtered_issue(issue, log_attachments)
                    issues_with_logs.append(filtered_issue)
        
        print(f"Found {len(issues_with_logs)} issues with log attachments")
        
        # Save filtered results
        output_file = self.config.outputs_dir / "issues_with_logs.json"
        save_json(issues_with_logs, output_file)
        print(f"Results saved to: {output_file}")
        
        # Display statistics
        self._print_statistics(issues, issues_with_logs)
        
        return output_file
    
    def _find_log_attachments(self, attachments: List[Dict]) -> List[Dict]:
        """Find attachments that appear to be log files."""
        log_attachments = []
        
        for attachment in attachments:
            if isinstance(attachment, dict) and "filename" in attachment:
                if "log" in attachment["filename"].lower():
                    log_attachments.append(attachment)
        
        return log_attachments
    
    def _create_filtered_issue(self, issue: Dict, log_attachments: List[Dict]) -> Dict:
        """Create a simplified issue entry with only relevant information."""
        return {
            "Issue key": issue["Issue key"],
            "Summary": issue["Summary"],
            "Status": issue["Status"],
            "Project name": issue["Project name"],
            "Priority": issue["Priority"], 
            "Created": issue["Created"],
            "Affects Version/s": issue.get("Affects Version/s", []),
            "Log attachments": log_attachments
        }
    
    def _print_statistics(self, all_issues: List[Dict], filtered_issues: List[Dict]) -> None:
        """Print filtering statistics."""
        print(f"\nStatistics:")
        print(f"- Total issues processed: {len(all_issues)}")
        print(f"- Issues with log attachments: {len(filtered_issues)}")
        
        # Count by project
        project_counts = defaultdict(int)
        for issue in filtered_issues:
            project = issue["Project name"]
            project_counts[project] += 1
        
        print(f"\nLog attachments by project:")
        for project, count in sorted(project_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {project}: {count} issues")