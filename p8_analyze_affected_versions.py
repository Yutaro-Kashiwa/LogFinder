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
            analysis_info = issue_data.get('analysis_results', {})
            blame_success = False

            for analysis_result in analysis_info:
                blame_result = analysis_result.get('changes')
                if len(blame_result) == 0:
                    continue
                else:
                    blame_success = True
                    break
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
                project_stats[project]['issues_without_affect_versions'] += 1
            else:
                for version in affected_versions:
                    if version:  # Skip empty versions
                        project_stats[project]['version_counts'][version] += 1
    

    

    
    # Convert defaultdicts to regular dicts for JSON serialization
    json_stats = {}
    for project, stats in project_stats.items():
        json_stats[project] = {
            'total_issues': stats['total_issues'],
            'version_counts': dict(stats['version_counts']),
            'issues_without_versions': stats['issues_without_versions'],
            'version_to_issues': {v: issues for v, issues in stats['version_counts'].items()}
        }
    # Save detailed statistics
    output_file = "outputs/p8_affected_versions_analysis.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(json_stats, f, indent=2)

    # Save detailed statistics
    output_file = "outputs/p8_issues_with_impacted_lines.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    
    print(f"\n\nDetailed analysis saved to: {output_file}")
    
    # Create summary CSV
    csv_file = "outputs/p6_affected_versions_summary.csv"
    with open(csv_file, 'w', encoding='utf-8') as f:
        f.write("Project,Version,Issue Count,Percentage,Sample Issues\n")
        
        for project, stats in project_stats.items():
            sorted_versions = sorted(stats['version_counts'].items(), 
                                   key=lambda x: x[1], reverse=True)
            
            for version, count in sorted_versions:  # Top 10 per project
                percentage = (count / stats['total_issues']) * 100
                sample_issues = ", ".join([issue['key'] for issue in stats['version_to_issues'][version][:3]])
                f.write(f'"{project}","{version}",{count},{percentage:.1f},"{sample_issues}"\n')
    
    print(f"Summary CSV saved to: {csv_file}")

if __name__ == "__main__":
    # Default to p4_issues_with_deleted_chunks.json
    input_file = "outputs/p7_test_data_with_diff.json"
    
    # Allow custom input file as argument
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    analyze_affected_versions(input_file)