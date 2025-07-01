#!/usr/bin/env python3
import json
import csv
from pathlib import Path
from collections import defaultdict

def export_issues_to_csv():
    input_file = "outputs/p8_issues_with_impacted_lines.json"
    
    if not Path(input_file).exists():
        print(f"Error: {input_file} not found. Please run other python files first.")
        return
    
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # The p4 JSON structure is: {project: {issue_key: {issue: {}, commits: []}}}
    # Convert to flat structure organized by project
    issues_by_project = {}
    
    for project, project_data in data.items():
        issues_by_project[project] = []
        print(project_data)
        for issue_id, issues in project_data.items():
            issue_data = issues['issue']
            # Extract issue info and add commit data
            for analysis_results in issues["analysis_results"]:
                if "error" in analysis_results:
                    continue
                issue_info = issue_data.copy()
                issue_info["affected_version"] = analysis_results["affected_version"]
                issue_info["affected_version_sha"] = analysis_results["affected_version_sha"]
                issue_info["affected_version_url"] = analysis_results["affected_version_url"]
                issue_info["fixing_commit_sha"] = analysis_results["fixing_commit_sha"]
                issue_info["fixing_commit_url"] = analysis_results["fixing_commit_url"]
                issues_by_project[project].append(issue_info)
    
        # Create CSV files for each project
        for project, project_issues in issues_by_project.items():
            # Create safe filename
            safe_project_name = project.replace("/", "_").replace(" ", "_")
            output_file = f"outputs/p9_{safe_project_name}_issues_with_logs.csv"

            # Define CSV headers
            headers = [
                "Issue key",
                "Issue Link",
                "Summary",
                "Status",
                "Priority",
                "Affect Version",
                "Created",
                "Affected version",
                "Affected version url",
                "Fixing commit url"
            ]

            # Write CSV file
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)

                # Write data rows
                for issue in project_issues:
                    issue_key = issue.get("key", "")
                    issue_link = f"https://issues.apache.org/jira/browse/{issue_key}" if issue_key else ""

                    row = [
                        issue_key,
                        issue_link,
                        issue.get("summary", ""),
                        issue.get("status", ""),
                        issue.get("priority", ""),
                        issue.get("affects", ""),
                        issue.get("created", ""),
                        issue.get("affected_version"),
                        issue.get("affected_version_url"),
                        issue.get("fixing_commit_url"),
                    ]

                    writer.writerow(row)

            print(f"Created: {output_file} ({len(project_issues)} issues)")

        # Also create a summary CSV with basic statistics
        summary_file = "outputs/p9_summary_by_project.csv"
        with open(summary_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                "Project",
                "Total Issues",
                "Issues with Commits",
                "Total Commits Found",
                "Avg Commits per Issue",
                "Most Common Status",
                "Most Common Priority"
            ])

            for project, project_issues in issues_by_project.items():
                total_commits = sum(issue.get("commit_count", 0) for issue in project_issues)
                issues_with_commits = sum(1 for issue in project_issues if issue.get("commit_count", 0) > 0)
                avg_commits = round(total_commits / len(project_issues), 2) if project_issues else 0

                # Count statuses and priorities
                status_counts = defaultdict(int)
                priority_counts = defaultdict(int)

                for issue in project_issues:
                    status_counts[issue.get("status", "")] += 1
                    priority_counts[issue.get("priority", "")] += 1

                most_common_status = max(status_counts.items(), key=lambda x: x[1])[0] if status_counts else ""
                most_common_priority = max(priority_counts.items(), key=lambda x: x[1])[0] if priority_counts else ""

                writer.writerow([
                    project,
                    len(project_issues),
                    issues_with_commits,
                    total_commits,
                    avg_commits,
                    most_common_status,
                    most_common_priority
                ])

        print(f"\nCreated summary: {summary_file}")
        print(f"\nTotal files created: {len(issues_by_project) + 1}")

if __name__ == "__main__":
    export_issues_to_csv()