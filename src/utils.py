#!/usr/bin/env python3
"""
Shared utilities for LogFinder project.
"""
import json
import csv
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional


class Config:
    """Configuration management for LogFinder."""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent
        self.inputs_dir = self.base_dir / "inputs"
        self.outputs_dir = self.base_dir / "outputs" 
        self.repos_dir = self.base_dir / "repos"
        
        # Repository configurations
        self.repositories = {
            "HBase": {
                "url": "https://github.com/apache/hbase.git",
                "local_path": str(self.repos_dir / "hbase"),
                "github_url": "https://github.com/apache/hbase"
            },
            "ZooKeeper": {
                "url": "https://github.com/apache/zookeeper.git", 
                "local_path": str(self.repos_dir / "zookeeper"),
                "github_url": "https://github.com/apache/zookeeper"
            }
        }
        
        # CSV field size limit
        csv.field_size_limit(sys.maxsize)
        
        # Ensure directories exist
        self.outputs_dir.mkdir(exist_ok=True)
        self.repos_dir.mkdir(exist_ok=True)


def load_json(file_path: Path) -> Any:
    """Load JSON data from file with error handling."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {file_path}: {e}")


def save_json(data: Any, file_path: Path) -> None:
    """Save data to JSON file with pretty formatting."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        raise IOError(f"Failed to save JSON to {file_path}: {e}")


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date string to datetime object for sorting."""
    try:
        return datetime.strptime(date_str, "%d/%b/%y %H:%M")
    except (ValueError, TypeError):
        return None


def safe_filename(name: str) -> str:
    """Create safe filename from project name."""
    return name.replace("/", "_").replace(" ", "_")


def print_progress(current: int, total: int, prefix: str = "") -> None:
    """Print progress indicator."""
    print(f"  [{current}/{total}] {prefix}", end='', flush=True)


def print_summary(title: str, data: Dict[str, Any]) -> None:
    """Print formatted summary information."""
    print(f"\n{title}:")
    for key, value in data.items():
        print(f"  - {key}: {value}")


class CSVExporter:
    """Handle CSV export operations."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def export_issues_with_commits(self, data: Dict[str, Any], filename_prefix: str = "issues_with_commits") -> List[Path]:
        """Export issues with commit data to CSV files by project."""
        output_files = []
        
        for project, project_data in data.items():
            # Convert nested structure to flat list
            issues = []
            for issue_key, issue_data in project_data.items():
                issue_info = issue_data["issue"].copy()
                issue_info["commits"] = issue_data["commits"] 
                issue_info["commit_count"] = issue_data["commit_count"]
                issues.append(issue_info)
            
            # Create CSV file
            safe_project = safe_filename(project)
            output_file = self.config.outputs_dir / f"{filename_prefix}_{safe_project}.csv"
            
            self._write_issues_csv(issues, output_file)
            output_files.append(output_file)
            print(f"Created: {output_file} ({len(issues)} issues)")
        
        return output_files
    
    def _write_issues_csv(self, issues: List[Dict], output_file: Path) -> None:
        """Write issues data to CSV file."""
        headers = [
            "Issue key", "Issue Link", "Summary", "Status", "Priority", 
            "Created", "Commit Count", "Commit SHAs", "Commit GitHub URLs"
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            
            for issue in issues:
                issue_key = issue.get("key", "")
                issue_link = f"https://issues.apache.org/jira/browse/{issue_key}" if issue_key else ""
                
                # Extract commit information
                commits = issue.get("commits", [])
                commit_shas = "; ".join([c.get("sha", "") for c in commits])
                commit_urls = "; ".join([c.get("github_url", "") for c in commits])
                
                row = [
                    issue_key,
                    issue_link, 
                    issue.get("summary", ""),
                    issue.get("status", ""),
                    issue.get("priority", ""),
                    issue.get("created", ""),
                    issue.get("commit_count", 0),
                    commit_shas,
                    commit_urls
                ]
                
                writer.writerow(row)
    
    def export_summary(self, data: Dict[str, Any], filename: str = "summary_by_project.csv") -> Path:
        """Export project summary statistics to CSV."""
        output_file = self.config.outputs_dir / filename
        
        headers = [
            "Project", "Total Issues", "Issues with Commits", "Total Commits Found", 
            "Avg Commits per Issue", "Most Common Status", "Most Common Priority"
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            
            for project, project_data in data.items():
                # Convert to flat structure for analysis
                issues = []
                for issue_data in project_data.values():
                    issue_info = issue_data["issue"].copy()
                    issue_info["commit_count"] = issue_data["commit_count"]
                    issues.append(issue_info)
                
                # Calculate statistics
                total_commits = sum(issue.get("commit_count", 0) for issue in issues)
                issues_with_commits = sum(1 for issue in issues if issue.get("commit_count", 0) > 0)
                avg_commits = round(total_commits / len(issues), 2) if issues else 0
                
                # Find most common status and priority
                from collections import defaultdict
                status_counts = defaultdict(int)
                priority_counts = defaultdict(int)
                
                for issue in issues:
                    status_counts[issue.get("status", "")] += 1
                    priority_counts[issue.get("priority", "")] += 1
                
                most_common_status = max(status_counts.items(), key=lambda x: x[1])[0] if status_counts else ""
                most_common_priority = max(priority_counts.items(), key=lambda x: x[1])[0] if priority_counts else ""
                
                writer.writerow([
                    project,
                    len(issues),
                    issues_with_commits,
                    total_commits,
                    avg_commits,
                    most_common_status,
                    most_common_priority
                ])
        
        print(f"Created summary: {output_file}")
        return output_file