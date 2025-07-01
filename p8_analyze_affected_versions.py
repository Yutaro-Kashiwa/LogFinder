#!/usr/bin/env python3
import json
from pathlib import Path
from collections import defaultdict
import sys

def analyze_affected_versions(input_file):
    """Analyze which affected versions are most prevalent across issues for each project"""
    
    if not Path(input_file).exists():
        print(f"Error: {input_file} not found!")
        return
    
    print(f"Reading from: {input_file}")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Initialize statistics
    project_stats = {}
    
    # Process each project
    for project, issues in data.items():
        project_stats[project] = {
            'total_issues': len(issues),
            'version_counts': defaultdict(int),
            'issues_without_versions': 0,
            'version_to_issues': defaultdict(list)
        }

        remove_issue_key = []
        for issue_key, issue_data in issues.items():
            analysis_info = issue_data.get('blame', {})
            blame_success = False

            for affected_version in analysis_info:
                blame_result = analysis_info.get(affected_version)
                commits = blame_result.get('commits')
                if len(commits) == 0:
                    continue
                else:
                    blame_success = True
            if not blame_success:
                remove_issue_key.append(issue_key)
        print(project, "before", len(issues))
        for issue_key in remove_issue_key:
            issues.pop(issue_key, None)
        print(project, "after", len(issues))
        for issue_key, issue_data in issues.items():

            issue_info = issue_data.get('issue', {})
            affected_versions = issue_info.get('affects', [])
            
            if not affected_versions:
                project_stats[project]['issues_without_versions'] += 1
            else:
                for version in affected_versions:
                    if version:  # Skip empty versions
                        project_stats[project]['version_counts'][version] += 1
                        project_stats[project]['version_to_issues'][version].append({
                            'key': issue_key,
                            'summary': issue_info.get('summary', 'No summary'),
                            'priority': issue_info.get('priority', 'Unknown')
                        })
    
    # Print results
    print("\n" + "=" * 80)
    print("AFFECTED VERSIONS ANALYSIS - ISSUES WITH DELETED CHUNKS")
    print("=" * 80)
    
    for project in sorted(project_stats.keys()):
        stats = project_stats[project]
        
        print(f"\n{'=' * 80}")
        print(f"PROJECT: {project}")
        print(f"{'=' * 80}")
        print(f"Total issues with deleted chunks: {stats['total_issues']}")
        print(f"Issues without version info: {stats['issues_without_versions']}")
        
        # Sort versions by count
        sorted_versions = sorted(stats['version_counts'].items(), 
                               key=lambda x: x[1], reverse=True)
        
        if sorted_versions:
            print(f"\n--- TOP AFFECTED VERSIONS ---")
            print(f"{'Version':<25} {'Count':>10} {'Percentage':>12}")
            print("-" * 50)
            
            for version, count in sorted_versions[:20]:  # Top 20 versions
                percentage = (count / stats['total_issues']) * 100
                print(f"{version:<25} {count:>10} {percentage:>11.1f}%")
            
            # Show details for top 5 versions
            print(f"\n--- ISSUE DETAILS FOR TOP 5 VERSIONS ---")
            for version, count in sorted_versions[:5]:
                print(f"\n{version} ({count} issues):")
                issues = stats['version_to_issues'][version]
                
                # Group by priority
                priority_groups = defaultdict(list)
                for issue in issues:
                    priority_groups[issue['priority']].append(issue)
                
                # Show issues by priority
                priority_order = ["Blocker", "Critical", "Major", "Minor", "Trivial"]
                for priority in priority_order:
                    if priority in priority_groups:
                        print(f"  {priority}:")
                        for issue in priority_groups[priority][:3]:  # Show max 3 per priority
                            print(f"    - {issue['key']}: {issue['summary'][:60]}...")
                        if len(priority_groups[priority]) > 3:
                            print(f"    ... and {len(priority_groups[priority]) - 3} more")
        else:
            print("\nNo version information available for issues in this project.")
    
    # Save detailed statistics
    output_file = "outputs/p8_affected_versions_analysis.json"
    
    # Convert defaultdicts to regular dicts for JSON serialization
    json_stats = {}
    for project, stats in project_stats.items():
        json_stats[project] = {
            'total_issues': stats['total_issues'],
            'version_counts': dict(stats['version_counts']),
            'issues_without_versions': stats['issues_without_versions'],
            'version_to_issues': {v: issues for v, issues in stats['version_to_issues'].items()}
        }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(json_stats, f, indent=2)
    
    print(f"\n\nDetailed analysis saved to: {output_file}")
    
    # Create summary CSV
    csv_file = "outputs/p6_affected_versions_summary.csv"
    with open(csv_file, 'w', encoding='utf-8') as f:
        f.write("Project,Version,Issue Count,Percentage,Sample Issues\n")
        
        for project, stats in project_stats.items():
            sorted_versions = sorted(stats['version_counts'].items(), 
                                   key=lambda x: x[1], reverse=True)
            
            for version, count in sorted_versions[:10]:  # Top 10 per project
                percentage = (count / stats['total_issues']) * 100
                sample_issues = ", ".join([issue['key'] for issue in stats['version_to_issues'][version][:3]])
                f.write(f'"{project}","{version}",{count},{percentage:.1f},"{sample_issues}"\n')
    
    print(f"Summary CSV saved to: {csv_file}")

if __name__ == "__main__":
    # Default to p4_issues_with_deleted_chunks.json
    input_file = "outputs/p7_test_data_with_blame.json"
    
    # Allow custom input file as argument
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    analyze_affected_versions(input_file)