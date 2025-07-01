#!/usr/bin/env python3
"""
Create test data by identifying which lines in the affected version are changed by fix commits.
Uses git diff with rename detection to map changes between affected versions and fix commits.
"""
import git
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set
import tempfile
from datetime import datetime
import re
from functools import wraps

from src.utils import Config, load_json, save_json, print_progress


def handle_git_errors(operation_name: str):
    """Decorator to handle git command errors consistently."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except git.exc.GitCommandError as e:
                print(f"\nWarning: {operation_name} failed: {str(e)[:200]}")
                return None if func.__name__.startswith('get_') else False
            except Exception as e:
                print(f"\nError in {operation_name}: {str(e)[:200]}")
                return None if func.__name__.startswith('get_') else False
        return wrapper
    return decorator


class VersionResolver:
    """Handle version to commit SHA resolution with project-specific logic."""
    
    def __init__(self, repo: git.Repo):
        self.repo = repo
        self.tag_prefixes = self._detect_tag_prefixes()
    
    def _detect_tag_prefixes(self) -> List[str]:
        """Get project-specific tag prefixes based on repository."""
        try:
            remote_url = self.repo.remotes.origin.url.lower()
        except:
            remote_url = str(self.repo.working_dir).lower()
        
        if 'zookeeper' in remote_url:
            return ['release-', 'RELEASE-', '']
        elif 'hbase' in remote_url:
            return ['rel/', 'REL/', '']
        else:
            return ['v', 'V', 'release-', 'rel/', '']
    
    @handle_git_errors("Version resolution")
    def resolve(self, version: str) -> Optional[str]:
        """Resolve version string to commit SHA."""
        # Try exact match first
        if version in self.repo.tags:
            return self.repo.tags[version].commit.hexsha
        
        # Try with project-specific prefixes
        for prefix in self.tag_prefixes:
            tag_name = f"{prefix}{version}"
            if tag_name in self.repo.tags:
                print(f"    Found tag: {tag_name}")
                return self.repo.tags[tag_name].commit.hexsha
        
        # Try as branch
        if version in [ref.name for ref in self.repo.heads]:
            return self.repo.heads[version].commit.hexsha
        
        # Try to resolve as any ref
        try:
            return self.repo.git.rev_parse(version)
        except:
            self._debug_available_tags(version)
            return None
    
    def _debug_available_tags(self, version: str):
        """Show available tags for debugging."""
        print(f"    Available tags containing '{version}':")
        matching_tags = [tag.name for tag in self.repo.tags if version in tag.name]
        for tag in matching_tags[:5]:
            print(f"      - {tag}")
        if len(matching_tags) > 5:
            print(f"      ... and {len(matching_tags) - 5} more")


class RepositoryValidator:
    """Validate repository configuration and availability."""
    
    @staticmethod
    def validate_config(project: str, config: Config) -> Optional[Dict[str, str]]:
        """Validate repository configuration exists."""
        repo_config = config.repositories.get(project)
        if not repo_config:
            print(f"Warning: No repository configuration for {project}")
        return repo_config
    
    @staticmethod
    def validate_path(repo_path: str) -> bool:
        """Validate repository path exists."""
        if not Path(repo_path).exists():
            print(f"Warning: Repository not found at {repo_path}")
            return False
        return True
    
    @staticmethod
    @handle_git_errors("Repository validation")
    def open_repository(repo_path: str) -> Optional[git.Repo]:
        """Open and validate git repository."""
        return git.Repo(repo_path)
    
    @staticmethod
    def check_missing_repositories(config: Config) -> List[str]:
        """Check for missing repositories and return list of missing ones."""
        missing = []
        for project, repo_config in config.repositories.items():
            if not Path(repo_config['local_path']).exists():
                missing.append(project)
        return missing


class GitDiffAnalyzer:
    """Use git diff to find changes between affected version and fix commit."""
    
    def __init__(self, repo: git.Repo, project:str):
        self.repo = repo
        self.version_resolver = VersionResolver(repo)
        self.project = project
    
    @handle_git_errors("Checkout commit")
    def checkout_commit(self, commit_sha: str) -> bool:
        """Checkout a specific commit."""
        self.repo.git.checkout(commit_sha)
        return True
    
    def parse_diff_output(self, diff_output: str) -> Dict[str, Any]:
        """Parse git diff output to extract changed lines."""
        results = {}
        current_file = None
        current_old_start = 0
        current_new_start = 0
        deleted_lines = []
        added_lines = []
        
        for line in diff_output.split('\n'):
            # File header
            if line.startswith('diff --git'):
                # Save previous file results
                if current_file and deleted_lines:
                    results[current_file] = {
                        'deleted_lines': deleted_lines,
                        'added_lines': added_lines
                    }
                # Reset for new file
                current_file = None
                deleted_lines = []
                added_lines = []
                
            # Extract filename from --- or +++ lines
            elif line.startswith('--- a/'):
                current_file = line[6:]
            elif line.startswith('--- /dev/null'):
                current_file = None  # New file, skip
            elif line.startswith('+++ b/') and not current_file:#TODO: いる？
                current_file = line[6:]  # For renamed files
                
            # Hunk header
            elif line.startswith('@@'):
                match = re.match(r'@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@', line)
                if match:
                    current_old_start = int(match.group(1))
                    current_new_start = int(match.group(2))
                    line_offset = 0
                    
            # Deleted line
            elif line.startswith('-') and not line.startswith('---'):
                if current_file:
                    deleted_lines.append({
                        'line_number': current_old_start + line_offset,
                        'content': line[1:]
                    })
                line_offset += 1
                
            # Added line
            elif line.startswith('+') and not line.startswith('+++'):
                if current_file:
                    added_lines.append({
                        'line_number': current_new_start + len(added_lines),
                        'content': line[1:]
                    })
            
            # Context line
            elif line.startswith(' '):
                line_offset += 1
        
        # Save last file
        if current_file and deleted_lines:
            results[current_file] = {
                'deleted_lines': deleted_lines,
                'added_lines': added_lines
            }
        
        return results
    
    def analyze_fix_commit(self, fix_commit_sha: str, affected_version: str, 
                          commit_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze changes between affected version and fix commit."""
        affected_version_sha = self.version_resolver.resolve(affected_version)
        if not affected_version_sha:
            return {
                'error': f'Could not resolve version {affected_version} to SHA',
                'affected_version': affected_version,
                'affected_version_sha': None,
                'fixing_commit_sha': fix_commit_sha,
                'changes': []
            }
        
        result = {
            'affected_version': affected_version,
            'affected_version_sha': affected_version_sha,
            'affected_version_url': f"https://github.com/apache/{self.project}/commit/{affected_version_sha}",
            'fixing_commit_sha': fix_commit_sha,
            'fixing_commit_url': f"https://github.com/apache/{self.project}/commit/{fix_commit_sha}",
            'checkout_command': f'git checkout {fix_commit_sha}',
            'changes': []
        }
        
        # Get files changed in the fix commit
        fix_files = set()
        for file_data in commit_data.get('files_changed', {}).get('files', []):
            if file_data.get('change_type') in ['MODIFY', 'DELETE']:
                fix_files.add(file_data['path'])
        
        # Run git diff for each file
        for file_path in fix_files:
            diff_command = f'git diff {affected_version_sha} {fix_commit_sha} -- {file_path}'
            print(diff_command)
            try:
                # Run git diff with rename detection
                diff_output = self.repo.git.diff(
                    affected_version_sha,
                    fix_commit_sha,
                    '--',
                    file_path,
                    find_renames=True,
                    no_prefix=False
                )
                if not diff_output:
                    raise
                
                # Parse diff output
                diff_results = self.parse_diff_output(diff_output)
                
                # Match deleted lines with fix commit changes
                file_result = self._match_with_fix_commit(
                    file_path, diff_results, commit_data, affected_version_sha, fix_commit_sha
                )
                
                if file_result:
                    file_result['diff_command'] = diff_command
                    result['changes'].append(file_result)
                    
            except Exception as e:
                print(f"    Error analyzing {file_path}: {e}")
                continue
        
        return result
    
    def _match_with_fix_commit(self, file_path: str, diff_results: Dict[str, Any],
                              commit_data: Dict[str, Any], affected_version_sha: str,
                              fix_commit_sha: str) -> Optional[Dict[str, Any]]:
        """Match diff results with fix commit changes."""
        if file_path not in diff_results:
            return None
        if not file_path.endswith('.java'):
            return None
        # Get the specific file changes from commit data
        file_changes = None
        for file_data in commit_data.get('files_changed', {}).get('files', []):
            if file_data['path'] == file_path:
                file_changes = file_data
                break
        
        if not file_changes:
            return None
        
        # Extract line numbers of deletions in affected version
        modified_lines = []
        unidentified_lines = []
        
        deleted_lines_diff = diff_results[file_path].get('deleted_lines', [])
        

            
        # Try to match with chunks in fix commit
        for chunk in file_changes.get('chunks', []):
            for change in chunk.get('changes', []):
                line_matched = False
                if change['type'] in ['DELETE', 'MODIFY']:
                    # Check each deleted line from diff against fix commit chunks
                    for deleted_line in deleted_lines_diff:
                        # Check if content matches
                        if change.get('content', '').strip() == deleted_line['content'].strip():
                            modified_lines.append(deleted_line['line_number'])
                            line_matched = True
                            break

                    if not line_matched:
                        unidentified_lines.append(change['line_number'])
                        pass

        
        result = {
            'filename': file_path,
            'affected_version': {
                'filename': file_path,
                'modified_lines': sorted(modified_lines)
            },
            'fixing_commit': {
                'filename': file_path,
                'unidentified_lines': sorted(unidentified_lines)
            }
        }
        
        return result


def process_single_issue(repo: git.Repo, issue_key: str, issue_data: Dict[str, Any], project:str) -> Optional[Dict[str, Any]]:
    """Process a single issue and return the analysis result."""
    # Get affected versions
    affected_versions = issue_data.get('issue', {}).get('affects', [])
    if not affected_versions:
        print(f"\n  Warning: No affected versions for {issue_key}")
        return None
    
    results = []
    
    # Process each affected version
    for affected_version in affected_versions:
        # Create a temporary clone to work with
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_repo_path = Path(temp_dir) / "repo"
            # Clone the repository to temporary directory
            print(f"\n  Cloning repository for {issue_key} (version: {affected_version})...")
            temp_repo = git.Repo.clone_from(
                str(repo.working_dir),
                str(temp_repo_path),
                no_checkout=False
            )
            
            analyzer = GitDiffAnalyzer(temp_repo, project)
            
            # Process each fix commit
            for commit in issue_data.get('commits', []):
                fix_commit_sha = commit['full_sha']
                print(f"  Processing fixing commit {fix_commit_sha}...")

                # Checkout the fix commit
                if analyzer.checkout_commit(fix_commit_sha):
                    # Analyze changes between affected version and fix commit
                    analysis_result = analyzer.analyze_fix_commit(
                        fix_commit_sha,
                        affected_version,
                        commit
                    )
                    
                    results.append(analysis_result)
    
    return {
        'issue': issue_data['issue'],
        'analysis_results': results
    }


def process_issues(input_file: Path, output_file: Path, config: Config) -> None:
    """Process all issues and create test data with diff analysis."""
    # Load input data
    data = load_json(input_file)
    
    # Validate repositories
    missing_repos = RepositoryValidator.check_missing_repositories(config)
    if missing_repos:
        print(f"Error: Missing repositories: {', '.join(missing_repos)}")
        print("Please clone the repositories first using:")
        for project in missing_repos:
            repo_config = config.repositories[project]
            print(f"  git clone {repo_config['url']} {repo_config['local_path']}")
        sys.exit(1)
    
    # Results will be stored here
    results = {}
    
    total_issues = sum(len(issues) for issues in data.values())
    processed = 0
    
    print(f"Processing {total_issues} issues with git diff analysis...")
    
    for project, issues in data.items():
        print(f"\nProcessing {project} ({len(issues)} issues)...")
        
        # Validate repository configuration
        repo_config = RepositoryValidator.validate_config(project, config)
        if not repo_config:
            continue
        
        repo_path = repo_config['local_path']
        if not RepositoryValidator.validate_path(repo_path):
            continue
        
        # Open repository
        repo = RepositoryValidator.open_repository(repo_path)
        if not repo:
            continue
        
        project_results = {}
        
        for issue_key, issue_data in issues.items():
            processed += 1
            print_progress(processed, total_issues, f"Processing {issue_key}")
            
            result = process_single_issue(repo, issue_key, issue_data, project)
            if result:
                project_results[issue_key] = result
            break  # FOR DEBUGGING
        if project_results:
            results[project] = project_results
        break # FOR DEBUGGING
    # Save results
    save_json(results, output_file)
    print(f"\n\nTest data saved to: {output_file}")
    
    # Print summary
    print_analysis_summary(results, total_issues)


def print_analysis_summary(results: Dict[str, Any], total_issues: int) -> None:
    """Print summary of analysis results."""
    total_analyzed = sum(len(project_data) for project_data in results.values())
    print(f"\nSummary:")
    print(f"  - Total issues processed: {total_issues}")
    print(f"  - Issues successfully analyzed: {total_analyzed}")
    
    for project, project_data in results.items():
        issues_with_lines = 0
        total_lines_found = 0
        total_unidentified = 0
        
        for issue_key, issue_result in project_data.items():
            has_lines = False
            for analysis in issue_result.get('analysis_results', []):
                for change in analysis.get('changes', []):
                    lines_count = len(change.get('affected_version', {}).get('modified_lines', []))
                    unidentified_count = len(change.get('fixing_commit', {}).get('unidentified_lines', []))
                    if lines_count > 0:
                        total_lines_found += lines_count
                        has_lines = True
                    total_unidentified += unidentified_count
            if has_lines:
                issues_with_lines += 1
        
        print(f"  - {project}: {len(project_data)} issues analyzed, "
              f"{issues_with_lines} with changed lines found, "
              f"{total_lines_found} total lines identified, "
              f"{total_unidentified} unidentified lines")


def main():
    """Main function."""
    config = Config()
    
    input_file = config.outputs_dir / "p4_issues_with_deleted_chunks.json"
    output_file = config.outputs_dir / "p7_test_data_with_diff.json"
    
    if not input_file.exists():
        print(f"Error: Input file {input_file} not found!")
        print("Please run p4_filter_issues_with_deleted_chunk.py first.")
        sys.exit(1)
    
    process_issues(input_file, output_file, config)


if __name__ == "__main__":
    main()