#!/usr/bin/env python3
"""
Collect GitHub issues that are closed and related to logs from specified projects.
Currently configured for Apache Druid, but can be extended to other projects.
"""
import requests
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import re
import os
import csv

from src.utils import Config, save_json


class GitHubIssueCollector:
    """Collect GitHub issues related to logs from specified repositories."""
    
    def __init__(self, github_token: Optional[str] = None):
        self.session = requests.Session()
        self.base_url = "https://api.github.com"
        self.rate_limit_remaining = 5000
        self.rate_limit_reset = 0
        self.request_cache = {}  # Simple cache for requests
        
        # Set up authentication if token provided
        if github_token:
            self.session.headers.update({
                'Authorization': f'token {github_token}',
                'Accept': 'application/vnd.github.v3+json',
                'User-Agent': 'LogFinder-Issue-Collector/1.0'
            })
        else:
            self.session.headers.update({
                'Accept': 'application/vnd.github.v3+json',
                'User-Agent': 'LogFinder-Issue-Collector/1.0'
            })
            print("Warning: No GitHub token provided. Rate limits will be lower (60 requests/hour).")
    
    def check_rate_limit(self):
        """Check and handle GitHub API rate limits."""
        if self.rate_limit_remaining <= 1:
            wait_time = max(self.rate_limit_reset - time.time(), 0) + 10
            print(f"Rate limit reached. Waiting {wait_time:.0f} seconds...")
            time.sleep(wait_time)
    
    def make_request(self, url: str, params: Dict = None) -> Optional[Dict]:
        """Make a GitHub API request with rate limit handling and caching."""
        # Create cache key
        cache_key = f"{url}?{str(sorted((params or {}).items()))}"
        
        # Check cache first
        if cache_key in self.request_cache:
            return self.request_cache[cache_key]
        
        self.check_rate_limit()
        
        try:
            response = self.session.get(url, params=params)
            
            # Update rate limit info
            self.rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
            self.rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', 0))
            
            if response.status_code == 200:
                result = response.json()
                # Cache the result (but limit cache size)
                if len(self.request_cache) < 1000:  # Prevent memory issues
                    self.request_cache[cache_key] = result
                return result
            elif response.status_code == 403:
                print(f"Rate limited or forbidden: {response.text}")
                time.sleep(60)
                return None
            elif response.status_code == 404:
                print(f"Not found: {url}")
                return None
            else:
                print(f"Error {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            print(f"Request failed: {e}")
            return None
    
    def get_paginated_results(self, url: str, params: Dict = None, max_results: Optional[int] = None) -> List[Dict]:
        """Get all pages of results from a paginated GitHub API endpoint."""
        all_results = []
        page = 1
        per_page = 100
        
        if params is None:
            params = {}
        params.update({'per_page': per_page, 'page': page})
        
        while True:
            print(f"  Fetching page {page}...")
            params['page'] = page
            
            data = self.make_request(url, params)
            if not data:
                break
                
            if isinstance(data, list):
                results = data
            else:
                results = data.get('items', [])
            
            if not results:
                break
                
            all_results.extend(results)
            
            # Check if we've reached the limit
            if max_results and len(all_results) >= max_results:
                all_results = all_results[:max_results]
                break
            
            # Check if we got a full page (if not, we're done)
            if len(results) < per_page:
                break
                
            page += 1
            
        return all_results
    
    def is_log_related(self, issue: Dict) -> bool:
        """Check if an issue is related to logs (has 'log' in description or log attachments)."""
        body = issue.get('body', '') or ''
        title = issue.get('title', '') or ''
        
        # Check if 'log' appears in title or body
        if 'log' in body.lower() or 'log' in title.lower():
            return True
        
        # Check for log file attachments
        log_attachment_patterns = [
            r'\[.*\]\(.*\.log\)',  # Markdown links to .log files
            r'https?://.*\.log',  # Direct .log file links
            r'\[.*log.*\]\(.*\)',  # Links with 'log' in the text
            r'log\.txt',  # log.txt files
            r'.*_log\.txt',  # files ending with _log.txt
            r'.*\.log\..*',  # log files with additional extensions
        ]
        
        for pattern in log_attachment_patterns:
            if re.search(pattern, body, re.IGNORECASE):
                return True
        
        return False
    
    def extract_attachments(self, issue: Dict) -> List[Dict]:
        """Extract attachment information from issue body."""
        body = issue.get('body', '') or ''
        attachments = []
        
        # Extract markdown images
        image_pattern = r'!\[(.*?)\]\((.*?)\)'
        for match in re.finditer(image_pattern, body):
            alt_text, url = match.groups()
            attachments.append({
                'type': 'image',
                'filename': alt_text or 'image',
                'url': url
            })
        
        # Extract file links
        file_pattern = r'\[(.*?)\]\((.*\.(log|txt|pdf|zip|tar\.gz|csv|json))\)'
        for match in re.finditer(file_pattern, body, re.IGNORECASE):
            filename, url, ext = match.groups()
            attachments.append({
                'type': 'file',
                'filename': filename,
                'url': url,
                'extension': ext
            })
        
        # Extract direct file URLs
        direct_file_pattern = r'(https?://.*\.(png|jpg|jpeg|gif|pdf|log|txt|zip|tar\.gz|csv|json))'
        for match in re.finditer(direct_file_pattern, body, re.IGNORECASE):
            url, ext = match.groups()
            filename = url.split('/')[-1]
            attachments.append({
                'type': 'file',
                'filename': filename,
                'url': url,
                'extension': ext
            })
        
        # Extract GitHub user uploads
        upload_pattern = r'(https://user-images\.githubusercontent\.com/[^\s)]+)'
        for match in re.finditer(upload_pattern, body):
            url = match.group(1)
            filename = url.split('/')[-1]
            attachments.append({
                'type': 'upload',
                'filename': filename,
                'url': url
            })
        
        return attachments
    
    def get_issue_details(self, owner: str, repo: str, issue_number: int) -> Optional[Dict]:
        """Get detailed information for a specific issue."""
        url = f"{self.base_url}/repos/{owner}/{repo}/issues/{issue_number}"
        return self.make_request(url)
    
    def find_closing_pr_optimized(self, owner: str, repo: str, issue_number: int) -> tuple[Optional[int], int]:
        """Find the pull request that closed the issue and return (pr_number, commit_count)."""
        # First try: search for PRs that mention this issue (most reliable and fewer calls)
        search_url = f"{self.base_url}/search/issues"
        search_params = {
            'q': f'repo:{owner}/{repo} is:pr is:merged #{issue_number}',
            'sort': 'updated',
            'order': 'desc'
        }
        
        result = self.make_request(search_url, search_params)
        if result and result.get('items'):
            # Return the first (most recent) PR that mentions this issue
            pr_data = result['items'][0]
            pr_number = pr_data['number']
            # Get commit count directly from the search result if available
            commit_count = pr_data.get('commits', 1)  # Default to 1 if not available
            return pr_number, commit_count
        
        # Fallback: check issue events (only if PR search failed)
        url = f"{self.base_url}/repos/{owner}/{repo}/issues/{issue_number}/events"
        events = self.make_request(url)
        
        if events:
            # Look for 'closed' event with a commit_id
            for event in events:
                if event.get('event') == 'closed' and event.get('commit_id'):
                    # Try to find the PR that contains this commit with a single search
                    search_params = {
                        'q': f'repo:{owner}/{repo} is:pr is:merged {event["commit_id"]}',
                        'sort': 'updated',
                        'order': 'desc'
                    }
                    
                    result = self.make_request(search_url, search_params)
                    if result and result.get('items'):
                        pr_data = result['items'][0]
                        pr_number = pr_data['number']
                        commit_count = pr_data.get('commits', 1)
                        return pr_number, commit_count
        
        return None, 0
    
    def get_issues_batch(self, owner: str, repo: str, issue_numbers: List[int]) -> Dict[int, Dict]:
        """Get details for multiple issues more efficiently."""
        issues_details = {}
        
        # Process in smaller batches to avoid rate limits
        batch_size = 10
        for i in range(0, len(issue_numbers), batch_size):
            batch = issue_numbers[i:i + batch_size]
            print(f"    Getting details for issues batch {i//batch_size + 1}")
            
            for issue_num in batch:
                issue_details = self.get_issue_details(owner, repo, issue_num)
                if issue_details:
                    issues_details[issue_num] = issue_details
                    
                # Small delay to be respectful of rate limits
                time.sleep(0.1)
        
        return issues_details
    
    def collect_issues_from_repo(self, owner: str, repo: str, max_issues: Optional[int] = None) -> List[Dict]:
        """Collect closed issues related to logs with state_reason 'completed' from a GitHub repository."""
        print(f"Collecting log-related issues from {owner}/{repo}...")
        
        # Search for closed issues with 'log' keyword
        search_url = f"{self.base_url}/search/issues"
        search_params = {
            'q': f'repo:{owner}/{repo} is:issue is:closed log',
            'sort': 'updated',
            'order': 'desc'
        }
        
        print("  Searching for closed issues...")
        issues = self.get_paginated_results(search_url, search_params, max_results=max_issues)
        
        # Limit issues if requested
        if max_issues and max_issues < len(issues):
            issues = issues[:max_issues]
            print(f"  Limited to first {max_issues} closed issues")
        else:
            print(f"  Found {len(issues)} closed issues")
        
        # First pass: filter log-related issues
        log_related_issue_numbers = []
        for i, issue in enumerate(issues):
            if i % 50 == 0:
                print(f"  Filtering issues: {i}/{len(issues)} (found {len(log_related_issue_numbers)} log-related so far)")
                
            # Check if it's log-related (cheap operation)
            if self.is_log_related(issue):
                log_related_issue_numbers.append(issue['number'])
        
        print(f"  Found {len(log_related_issue_numbers)} log-related issues")
        
        # Second pass: get details for log-related issues in batches
        log_related_issues = []
        issues_with_completed_state = 0
        
        if log_related_issue_numbers:
            print(f"  Getting detailed info for {len(log_related_issue_numbers)} log-related issues...")
            issues_details = self.get_issues_batch(owner, repo, log_related_issue_numbers)
            
            # Third pass: process issues with completed state and find PRs
            for issue_number, issue_details in issues_details.items():
                # Check if state_reason is 'completed' (not 'not_planned')
                state_reason = issue_details.get('state_reason')
                if state_reason == 'completed':
                    issues_with_completed_state += 1
                    
                    # Extract attachment details
                    attachments = self.extract_attachments(issue_details)
                    
                    # Find the PR that closed this issue (optimized - fewer API calls)
                    print(f"    Finding PR for issue #{issue_details['number']}...")
                    pr_number, commit_count = self.find_closing_pr_optimized(owner, repo, issue_details['number'])
                    pr_url = f"https://github.com/{owner}/{repo}/pull/{pr_number}" if pr_number else None
                    
                    if pr_number:
                        print(f"      Found PR #{pr_number} with {commit_count} commits")
                    else:
                        print(f"      No PR found for issue #{issue_details['number']}")
                    
                    issue_data = {
                        'key': f"{repo.upper()}-{issue_details['number']}",  # Create a consistent key format
                        'number': issue_details['number'],
                        'title': issue_details['title'],
                        'state': issue_details['state'],
                        'state_reason': state_reason,
                        'created_at': issue_details['created_at'],
                        'updated_at': issue_details['updated_at'],
                        'closed_at': issue_details['closed_at'],
                        'body': issue_details['body'],
                        'user': issue_details['user']['login'],
                        'labels': [label['name'] for label in issue_details['labels']],
                        'url': issue_details['html_url'],
                        'api_url': issue_details['url'],
                        'attachments': attachments,
                        'pr_number': pr_number,
                        'pr_url': pr_url,
                        'commit_count': commit_count
                    }
                    
                    log_related_issues.append(issue_data)
        
        print(f"  Checked {len(log_related_issue_numbers)} log-related issues")
        print(f"  Found {issues_with_completed_state} issues with state_reason='completed'")
        print(f"  Total matching issues: {len(log_related_issues)}")
        return log_related_issues
    
    def collect_issues(self, repositories: List[Dict[str, str]], max_issues_per_repo: Optional[int] = None) -> Dict[str, List[Dict]]:
        """Collect issues from multiple repositories."""
        all_issues = {}
        
        for repo_info in repositories:
            owner = repo_info['owner']
            repo = repo_info['repo']
            project_name = repo_info.get('project_name', repo)
            
            issues = self.collect_issues_from_repo(owner, repo, max_issues_per_repo)
            if issues:
                all_issues[project_name] = issues
        
        return all_issues


def get_github_token(interactive: bool = True) -> Optional[str]:
    """Get GitHub token from environment or prompt user."""
    # Try to get from environment variable
    token = os.getenv('GITHUB_TOKEN')
    if token:
        return token
    
    # Try to get from config file
    config_file = Path.home() / '.github_token'
    if config_file.exists():
        return config_file.read_text().strip()
    
    if not interactive:
        print("GitHub token not found. Running without authentication (lower rate limits).")
        return None
    
    # Prompt user
    print("GitHub token not found in environment (GITHUB_TOKEN) or ~/.github_token")
    print("You can continue without a token, but rate limits will be much lower.")
    try:
        use_token = input("Do you have a GitHub token? (y/n): ").lower().startswith('y')
        
        if use_token:
            token = input("Enter your GitHub token: ").strip()
            save_token = input("Save token to ~/.github_token? (y/n): ").lower().startswith('y')
            if save_token and token:
                config_file.write_text(token)
                config_file.chmod(0o600)  # Secure permissions
            return token
    except (EOFError, KeyboardInterrupt):
        print("\nNo token provided. Running without authentication.")
    
    return None


def save_as_csv(formatted_issues: Dict[str, Dict], output_file: Path) -> None:
    """Save formatted issues to a CSV file."""
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        # Define CSV headers
        fieldnames = [
            'project', 'issue_key', 'issue_number', 'title', 'status', 'state_reason',
            'priority', 'created', 'updated', 'closed', 'url', 'labels', 
            'user', 'body_preview', 'attachments_count', 'attachment_types',
            'pr_number', 'pr_url', 'commit_count'
        ]
        
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Write issues to CSV
        for project_name, project_issues in formatted_issues.items():
            for issue_key, issue_data in project_issues.items():
                issue = issue_data['issue']
                
                # Get unique attachment types
                attachment_types = set()
                for attachment in issue.get('attachments', []):
                    if 'extension' in attachment:
                        attachment_types.add(attachment['extension'])
                    elif 'type' in attachment:
                        attachment_types.add(attachment['type'])
                
                # Truncate body for CSV (to avoid huge cells)
                body = issue.get('body', '') or ''
                body_preview = body[:500] + '...' if len(body) > 500 else body
                # Remove newlines for CSV format
                body_preview = ' '.join(body_preview.split())
                
                row = {
                    'project': project_name,
                    'issue_key': issue['key'],
                    'issue_number': issue['key'].split('-')[-1],
                    'title': issue['summary'],
                    'status': issue['status'],
                    'state_reason': issue['state_reason'],
                    'priority': issue['priority'],
                    'created': issue['created'],
                    'updated': issue['updated'],
                    'closed': issue['closed'],
                    'url': issue['url'],
                    'labels': ', '.join(issue.get('labels', [])),
                    'user': issue.get('user', 'N/A'),
                    'body_preview': body_preview,
                    'attachments_count': len(issue.get('attachments', [])),
                    'attachment_types': ', '.join(sorted(attachment_types)),
                    'pr_number': issue.get('pr_number', ''),
                    'pr_url': issue.get('pr_url', ''),
                    'commit_count': issue.get('commit_count', 0)
                }
                
                writer.writerow(row)


def main():
    """Main function."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Collect GitHub issues related to logs')
    parser.add_argument('--no-interactive', action='store_true', help='Run without prompts')
    parser.add_argument('--max-issues', type=int, help='Maximum issues to process per repository')
    args = parser.parse_args()
    
    config = Config()
    
    # Get GitHub token
    github_token = get_github_token(interactive=not args.no_interactive)
    
    # Initialize collector
    collector = GitHubIssueCollector(github_token)
    
    # Define repositories to collect from
    repositories = [
        {
            'owner': 'apache',
            'repo': 'druid',
            'project_name': 'Druid'
        }
        # Add more repositories here as needed
        # {
        #     'owner': 'apache',
        #     'repo': 'kafka',
        #     'project_name': 'Kafka'
        # }
    ]
    
    print(f"Collecting issues from {len(repositories)} repositories...")
    if args.max_issues:
        print(f"Limited to {args.max_issues} issues per repository")
    
    # Collect issues
    all_issues = collector.collect_issues(repositories, max_issues_per_repo=args.max_issues)
    
    # Convert to the expected format
    formatted_issues = {}
    total_issues = 0
    
    for project_name, issues in all_issues.items():
        project_issues = {}
        for issue in issues:
            issue_key = issue['key']
            
            # Format to match existing structure
            formatted_issue = {
                'issue': {
                    'key': issue_key,
                    'summary': issue['title'],
                    'status': 'Closed',
                    'state_reason': issue['state_reason'],
                    'priority': 'Unknown',  # GitHub doesn't have priority field
                    'created': issue['created_at'],
                    'updated': issue['updated_at'],
                    'closed': issue['closed_at'],
                    'url': issue['url'],
                    'labels': issue['labels'],
                    'attachments': issue['attachments'],
                    # Include body and user for CSV export
                    'body': issue['body'],
                    'user': issue['user'],
                    # Include PR and commit information
                    'pr_number': issue.get('pr_number'),
                    'pr_url': issue.get('pr_url'),
                    'commit_count': issue.get('commit_count', 0)
                }
            }
            
            project_issues[issue_key] = formatted_issue
            total_issues += 1
        
        if project_issues:
            formatted_issues[project_name] = project_issues
    
    # Save results as JSON
    output_file = config.outputs_dir / "p1_github_issues.json"
    save_json(formatted_issues, output_file)
    
    # Save results as CSV
    csv_output_file = config.outputs_dir / "p1_github_issues.csv"
    save_as_csv(formatted_issues, csv_output_file)
    
    # Print summary
    print(f"\nCollected {total_issues} log-related issues")
    for project_name, issues in formatted_issues.items():
        print(f"  {project_name}: {len(issues)} issues")
    
    print(f"\nResults saved to:")
    print(f"  JSON: {output_file}")
    print(f"  CSV: {csv_output_file}")
    
    # Also create a summary file
    summary = {
        'collection_date': datetime.now().isoformat(),
        'total_issues': total_issues,
        'projects': {
            project: len(issues) for project, issues in formatted_issues.items()
        },
        'repositories': repositories
    }
    
    summary_file = config.outputs_dir / "p1_github_collection_summary.json"
    save_json(summary, summary_file)
    print(f"Summary saved to: {summary_file}")


if __name__ == "__main__":
    main()