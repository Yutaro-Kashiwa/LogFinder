#!/usr/bin/env python3


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


# !/usr/bin/env python3
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime


def parse_date(date_str):
    """Parse date string to datetime object for sorting"""
    try:
        return datetime.strptime(date_str, "%d/%b/%y %H:%M")
    except:
        return None


def analyze_issues_by_project():
    input_file = "outputs/p2_issues_with_logs.json"

    if not Path(input_file).exists():
        print(f"Error: {input_file} not found. Please run p2_find_log_attachments.py first.")
        return

    with open(input_file, 'r', encoding='utf-8') as f:
        issues = json.load(f)

    # Initialize project-specific statistics
    project_stats = {}

    # Group issues by project
    for issue in issues:
        project = issue["Project name"]

        if project not in project_stats:
            project_stats[project] = {
                "total_issues": 0,
                "by_status": defaultdict(int),
                "by_priority": defaultdict(int),
                "by_year": defaultdict(int),
                "by_affected_version": defaultdict(int),
                "log_files_count": 0,
                "avg_logs_per_issue": 0,
                "most_logs_in_issue": {"count": 0, "issue": None},
                "users_uploading_logs": defaultdict(int),
                "log_file_types": defaultdict(int),
                "issues_list": []
            }

        stats = project_stats[project]
        stats["total_issues"] += 1
        stats["issues_list"].append(issue["Issue key"])

        # Count by status and priority
        stats["by_status"][issue["Status"]] += 1
        stats["by_priority"][issue["Priority"]] += 1

        # Parse creation date for year statistics
        created_date = parse_date(issue["Created"])
        if created_date:
            stats["by_year"][created_date.year] += 1

        # Count affected versions
        affected_versions = issue.get("Affects Version/s", [])
        if affected_versions:
            for version in affected_versions:
                if version:  # Skip empty versions
                    stats["by_affected_version"][version] += 1

        # Count log attachments
        log_count = len(issue["Log attachments"])
        stats["log_files_count"] += log_count

        # Track issue with most logs
        if log_count > stats["most_logs_in_issue"]["count"]:
            stats["most_logs_in_issue"]["count"] = log_count
            stats["most_logs_in_issue"]["issue"] = issue["Issue key"]

        # Analyze log attachments
        for attachment in issue["Log attachments"]:
            # Track users
            username = attachment.get("username", "Unknown")
            stats["users_uploading_logs"][username] += 1

            # Analyze file extensions
            filename = attachment.get("filename", "")
            if "." in filename:
                extension = filename.split(".")[-1].lower()
                stats["log_file_types"][extension] += 1
            else:
                stats["log_file_types"]["no_extension"] += 1

    # Calculate averages for each project
    for project, stats in project_stats.items():
        stats["avg_logs_per_issue"] = round(stats["log_files_count"] / stats["total_issues"], 2) if stats[
                                                                                                        "total_issues"] > 0 else 0

    # Print statistics for each project
    print("=" * 80)
    print("ISSUES WITH LOG ATTACHMENTS - STATISTICS BY PROJECT")
    print("=" * 80)

    for project in sorted(project_stats.keys()):
        stats = project_stats[project]

        print(f"\n{'=' * 80}")
        print(f"PROJECT: {project}")
        print(f"{'=' * 80}")

        print(f"\nTOTAL ISSUES: {stats['total_issues']}")
        print(f"TOTAL LOG FILES: {stats['log_files_count']}")
        print(f"AVERAGE LOG FILES PER ISSUE: {stats['avg_logs_per_issue']}")

        print(f"\n--- ISSUES BY STATUS ({project}) ---")
        for status, count in sorted(stats["by_status"].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / stats["total_issues"]) * 100
            print(f"  {status:20} {count:4} ({percentage:5.1f}%)")

        print(f"\n--- ISSUES BY PRIORITY ({project}) ---")
        priority_order = ["Blocker", "Critical", "Major", "Minor", "Trivial"]
        for priority in priority_order:
            if priority in stats["by_priority"]:
                count = stats["by_priority"][priority]
                percentage = (count / stats["total_issues"]) * 100
                print(f"  {priority:20} {count:4} ({percentage:5.1f}%)")

        print(f"\n--- ISSUES BY YEAR ({project}) ---")
        for year, count in sorted(stats["by_year"].items()):
            percentage = (count / stats["total_issues"]) * 100
            print(f"  {year:20} {count:4} ({percentage:5.1f}%)")

        print(f"\n--- TOP AFFECTED VERSIONS ({project}) ---")
        top_versions = sorted(stats["by_affected_version"].items(), key=lambda x: x[1], reverse=True)[:15]
        if top_versions:
            for version, count in top_versions:
                percentage = (count / stats["total_issues"]) * 100
                print(f"  {version:20} {count:4} ({percentage:5.1f}%)")
        else:
            print("  No affected versions recorded")

        print(f"\n--- LOG FILE TYPES ({project}) ---")
        for ext, count in sorted(stats["log_file_types"].items(), key=lambda x: x[1], reverse=True)[:10]:
            percentage = (count / stats["log_files_count"]) * 100
            print(f"  .{ext:19} {count:4} ({percentage:5.1f}%)")

        print(f"\n--- TOP LOG UPLOADERS ({project}) ---")
        top_uploaders = sorted(stats["users_uploading_logs"].items(), key=lambda x: x[1], reverse=True)[:10]
        for username, count in top_uploaders:
            percentage = (count / stats["log_files_count"]) * 100
            print(f"  {username:20} {count:4} ({percentage:5.1f}%)")

        print(f"\n--- ISSUE WITH MOST LOGS ({project}) ---")
        print(f"  Issue: {stats['most_logs_in_issue']['issue']}")
        print(f"  Log count: {stats['most_logs_in_issue']['count']}")

    # Save detailed statistics to JSON
    output_file = "outputs/p2_project_statistics.json"

    # Convert defaultdicts to regular dicts for JSON serialization
    json_stats = {}
    for project, stats in project_stats.items():
        json_stats[project] = {
            "total_issues": stats["total_issues"],
            "by_status": dict(stats["by_status"]),
            "by_priority": dict(stats["by_priority"]),
            "by_year": dict(stats["by_year"]),
            "by_affected_version": dict(stats["by_affected_version"]),
            "log_files_count": stats["log_files_count"],
            "avg_logs_per_issue": stats["avg_logs_per_issue"],
            "most_logs_in_issue": stats["most_logs_in_issue"],
            "users_uploading_logs": dict(stats["users_uploading_logs"]),
            "log_file_types": dict(stats["log_file_types"]),
            "issues_list": stats["issues_list"]
        }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(json_stats, f, indent=2)

    print(f"\n\nDetailed statistics saved to: {output_file}")



if __name__ == "__main__":
    find_issues_with_log_attachments()
    analyze_issues_by_project()
