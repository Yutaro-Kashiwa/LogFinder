#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
import requests
from urllib.parse import unquote
import time

def sanitize_filename(filename):
    """Sanitize filename for filesystem compatibility"""
    # Remove or replace problematic characters
    invalid_chars = '<>:"|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    return filename

def download_file(url, filepath, max_retries=3):
    """Download a file from URL to filepath with retry logic"""
    for attempt in range(max_retries):
        try:
            print(f"  Downloading: {os.path.basename(filepath)}")
            
            # Note: JIRA attachments typically require authentication
            # For public JIRA instances, sometimes no auth is needed
            # You may need to add authentication headers here
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30, stream=True)
            
            if response.status_code == 200:
                # Create parent directory if it doesn't exist
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                
                # Write file in chunks
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                print(f"    ✓ Downloaded successfully")
                return True
            else:
                print(f"    ✗ HTTP {response.status_code}: {response.reason}")
                if response.status_code == 401:
                    print("    Note: Authentication may be required for JIRA attachments")
                    return False
                
        except requests.exceptions.RequestException as e:
            print(f"    ✗ Error: {str(e)}")
            if attempt < max_retries - 1:
                print(f"    Retrying... (attempt {attempt + 2}/{max_retries})")
                time.sleep(2 ** attempt)  # Exponential backoff
            
    return False

def download_logs_from_issues(input_file, output_dir):
    """Download log files from issues and organize by project/issue"""
    
    # Read the filtered issues
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    # Create base output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    total_files = 0
    downloaded_files = 0
    failed_files = 0
    
    print(f"Starting download of log files to: {output_dir}/")
    print("=" * 80)
    
    for project, issues in data.items():
        print(f"\nProject: {project}")
        print("-" * 40)
        
        # Create project directory
        project_dir = output_path / sanitize_filename(project)
        project_dir.mkdir(exist_ok=True)
        
        for issue_key, issue_data in issues.items():
            issue_info = issue_data.get('issue', {})
            log_attachments = issue_info.get('log', [])
            
            if not log_attachments:
                continue
            
            print(f"\nIssue: {issue_key} - {issue_info.get('summary', 'No summary')}")
            
            # Create issue directory
            issue_dir = project_dir / sanitize_filename(issue_key)
            issue_dir.mkdir(exist_ok=True)
            
            # Save issue metadata
            metadata_file = issue_dir / "issue_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump({
                    'issue_key': issue_key,
                    'summary': issue_info.get('summary'),
                    'status': issue_info.get('status'),
                    'priority': issue_info.get('priority'),
                    'created': issue_info.get('created'),
                    'affects': issue_info.get('affects', [])
                }, f, indent=2)
            
            # Download each log attachment
            for attachment in log_attachments:
                total_files += 1
                
                filename = attachment.get('filename', 'unknown.log')
                url = attachment.get('url', '')
                
                if not url:
                    print(f"  ✗ No URL for: {filename}")
                    failed_files += 1
                    continue
                
                # Sanitize filename
                safe_filename = sanitize_filename(filename)
                filepath = issue_dir / safe_filename
                
                # Skip if file already exists
                if filepath.exists():
                    print(f"  ⚠ Already exists: {safe_filename}")
                    downloaded_files += 1
                    continue
                
                # Download the file
                if download_file(url, filepath):
                    downloaded_files += 1
                else:
                    failed_files += 1
                    
                    # Save failed download info
                    failed_info = {
                        'filename': filename,
                        'url': url,
                        'issue': issue_key,
                        'project': project
                    }
                    
                    failed_file = output_path / "failed_downloads.json"
                    if failed_file.exists():
                        with open(failed_file, 'r') as f:
                            failed_list = json.load(f)
                    else:
                        failed_list = []
                    
                    failed_list.append(failed_info)
                    
                    with open(failed_file, 'w') as f:
                        json.dump(failed_list, f, indent=2)
    
    # Print summary
    print("\n" + "=" * 80)
    print("DOWNLOAD SUMMARY")
    print("=" * 80)
    print(f"Total files: {total_files}")
    print(f"Successfully downloaded: {downloaded_files}")
    print(f"Failed downloads: {failed_files}")
    print(f"Success rate: {(downloaded_files/total_files*100):.1f}%" if total_files > 0 else "N/A")
    
    if failed_files > 0:
        print(f"\nFailed downloads saved to: {output_path}/failed_downloads.json")
        print("Note: Failed downloads may be due to:")
        print("  - Authentication requirements")
        print("  - Expired/invalid URLs")
        print("  - Network issues")

if __name__ == "__main__":
    input_file = Path("outputs/p4_issues_with_deleted_chunks.json")
    output_dir = Path("downloads/logs")
    
    if not input_file.exists():
        print(f"Error: Input file {input_file} not found!")
        sys.exit(1)
    
    # Optional: Add command line arguments
    if len(sys.argv) > 1:
        output_dir = Path(sys.argv[1])
    
    download_logs_from_issues(input_file, output_dir)