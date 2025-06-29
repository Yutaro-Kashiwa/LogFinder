#!/usr/bin/env python3
from pathlib import Path

from src.statistics import analyze_issues_by_project
import json

def find_issues_with_log_attachments():
    # Read the issues file
    input_file = "outputs/p1_issues.json"

    if not Path(input_file).exists():
        print(f"Error: Neither issues.json nor processed_logs.json found.")
        return
    
    print(f"Reading from: {input_file}")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        issues = json.load(f)
    
    # Filter issues that have attachments with "log" in the filename
    issues_with_logs = []
    
    for issue in issues:
        if issue.get("Attachment"):
            log_attachments = []
            
            for attachment in issue["Attachment"]:
                # Check if attachment is a dict with filename field
                if isinstance(attachment, dict) and "filename" in attachment:
                    if "log" in attachment["filename"].lower():
                        log_attachments.append(attachment)
            
            if log_attachments:
                # Create a simplified issue entry with only relevant info
                filtered_issue = {
                    "Issue key": issue["Issue key"],
                    "Summary": issue["Summary"],
                    "Status": issue["Status"],
                    "Project name": issue["Project name"],
                    "Priority": issue["Priority"],
                    "Created": issue["Created"],
                    "Affects Version/s": issue.get("Affects Version/s", []),
                    "Log attachments": log_attachments
                }
                issues_with_logs.append(filtered_issue)
    
    print(f"Found {len(issues_with_logs)} issues with log attachments")
    
    # Save the filtered results
    output_file = "outputs/p2_issues_with_logs.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(issues_with_logs, f, indent=2, ensure_ascii=False)
    
    print(f"Results saved to: {output_file}")
    
    # Display some statistics
    print(f"\nStatistics:")
    print(f"- Total issues processed: {len(issues)}")
    print(f"- Issues with log attachments: {len(issues_with_logs)}")
    
    # Count by project
    project_counts = {}
    for issue in issues_with_logs:
        project = issue["Project name"]
        project_counts[project] = project_counts.get(project, 0) + 1
    
    print(f"\nLog attachments by project:")
    for project, count in sorted(project_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  - {project}: {count} issues")




if __name__ == "__main__":
    find_issues_with_log_attachments()
    analyze_issues_by_project()
