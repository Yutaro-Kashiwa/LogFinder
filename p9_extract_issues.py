#!/usr/bin/env python3
import json
import sys
from typing import List, Dict, Any

def extract_issues(json_file_path: str, issue_keys: List[str]) -> Dict[str, Any]:
    """
    Extract specific issues from the JSON file.
    
    Args:
        json_file_path: Path to the JSON file
        issue_keys: List of issue keys to extract (e.g., ["ZOOKEEPER-4293"])
    
    Returns:
        Dictionary containing only the requested issues
    """
    with open(json_file_path, 'r') as f:
        data = json.load(f)
    
    extracted = {}
    
    # Iterate through all projects in the JSON
    for project, issues in data.items():
        extracted_issues = {}
        
        # Check each requested issue key
        for issue_key in issue_keys:
            if issue_key in issues:
                extracted_issues[issue_key] = issues[issue_key]
                print(f"Found: {issue_key} in project {project}")
            else:
                # Try to find issue in this project with different formatting
                for key in issues.keys():
                    if key.upper() == issue_key.upper():
                        extracted_issues[issue_key] = issues[key]
                        print(f"Found: {issue_key} in project {project} (as {key})")
                        break
        
        if extracted_issues:
            extracted[project] = extracted_issues
    
    return extracted

def save_extracted_issues(extracted_data: Dict[str, Any], output_file: str):
    """Save extracted issues to a new JSON file."""
    with open(output_file, 'w') as f:
        json.dump(extracted_data, f, indent=2)
    print(f"\nExtracted issues saved to: {output_file}")

def save_individual_issue_files(extracted_data: Dict[str, Any], output_dir: str = "individual_issues"):
    """Save each issue to its own JSON file."""
    import os
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")
    
    saved_files = []
    
    # Save each issue to its own file
    for project, issues in extracted_data.items():
        for issue_key, issue_data in issues.items():
            # Create filename
            filename = f"{issue_key}.json"
            filepath = os.path.join(output_dir, filename)
            
            # Prepare data for individual file (include project context)
            individual_data = {
                "project": project,
                "issue_key": issue_key,
                "data": issue_data
            }
            
            # Save to file
            with open(filepath, 'w') as f:
                json.dump(individual_data, f, indent=2)
            
            saved_files.append(filepath)
            print(f"Saved: {filepath}")
    
    return saved_files

def print_issue_summary(extracted_data: Dict[str, Any]):
    """Print a summary of extracted issues."""
    print("\n=== EXTRACTED ISSUES SUMMARY ===")
    for project, issues in extracted_data.items():
        print(f"\nProject: {project}")
        for issue_key, issue_data in issues.items():
            issue_info = issue_data.get('issue', {})
            print(f"  - {issue_key}: {issue_info.get('summary', 'No summary')}")
            print(f"    Status: {issue_info.get('status', 'Unknown')}")
            print(f"    Priority: {issue_info.get('priority', 'Unknown')}")
            print(f"    Created: {issue_info.get('created', 'Unknown')}")
            
            # Count analysis results
            analysis_results = issue_data.get('analysis_results', [])
            print(f"    Analysis results: {len(analysis_results)} version(s)")

def main():
    # Define the issues to extract
    issue_keys = [
        "ZOOKEEPER-4293",
        "ZOOKEEPER-3829", 
        "ZOOKEEPER-3769",
        "ZOOKEEPER-3756",
        "HBASE-11906",
        "HBASE-17069",
        "HBASE-14291",
        "HBASE-20723",
        "HBASE-24813"
    ]
    
    # File paths
    input_file = "outputs/p8_issues_with_impacted_lines.json"
    output_file = "extracted_issues.json"
    
    print(f"Extracting issues: {', '.join(issue_keys)}")
    print(f"From file: {input_file}\n")
    
    try:
        # Extract issues
        extracted = extract_issues(input_file, issue_keys)
        
        if not extracted:
            print("No matching issues found!")
            return
        
        # Save to file
        save_extracted_issues(extracted, output_file)
        
        # Save individual issue files
        save_individual_issue_files(extracted)
        
        # Print summary
        print_issue_summary(extracted)
        
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found!")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format - {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()