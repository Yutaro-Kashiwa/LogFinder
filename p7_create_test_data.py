#!/usr/bin/env python3
"""
Create test data by identifying which lines in the affected version are changed by fix commits.
Uses git blame --reverse to identify the last change between the affected version and fix revision.
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


class GitBlameReverseAnalyzer:
    """Use git blame --reverse to map lines from affected version to fix commit."""
    
    def __init__(self, repo: git.Repo):
        self.repo = repo
        self.version_resolver = VersionResolver(repo)
    
    @handle_git_errors("Checkout version")
    def checkout_version(self, version: str) -> bool:
        """Checkout a specific version/tag."""
        version_sha = self.version_resolver.resolve(version)
        if version_sha:
            self.repo.git.checkout(version_sha)
            return True
        else:
            print(f"\nWarning: Could not resolve version {version}")
            return False
    
    def get_changed_lines_in_affected_version(self, file_path: str, fix_commit_sha: str, 
                                            affected_version: str, file_changes: Dict[str, Any], parent_commit_sha: str) -> List[Dict[str, Any]]:
        """
        Use git blame --reverse to find which lines in affected version were changed by fix commit.
        Returns list of line numbers in affected version that were modified.
        """
        affected_version_sha = self.version_resolver.resolve(affected_version)
        if not affected_version_sha:
            print(f"\n  Warning: Could not resolve version {affected_version} to SHA")
            return []
        
        try:
            # Check if file exists in affected version
            if not self._file_exists_at_commit(file_path, affected_version_sha):
                return []
            
            # Get the file content at affected version
            file_lines = self._get_file_at_commit(file_path, affected_version_sha)
            if not file_lines:
                return []
            
            # Run git blame --reverse to find last modifications between affected version and fix
            blame_data = self._run_reverse_blame(file_path, affected_version_sha, fix_commit_sha)
            
            # Map the changes from fix commit to lines in affected version
            changed_lines = self._map_changes_to_affected_version(
                blame_data, file_changes, file_lines, fix_commit_sha, parent_commit_sha
            )
            
            return changed_lines
            
        except Exception as e:
            print(f"\n  Error analyzing {file_path}: {e}")
            return []
    
    def _file_exists_at_commit(self, file_path: str, commit_sha: str) -> bool:
        """Check if file exists at specific commit."""
        try:
            commit = self.repo.commit(commit_sha)
            commit.tree[file_path]
            return True
        except (KeyError, AttributeError):
            return False
    
    def _get_file_at_commit(self, file_path: str, commit_sha: str) -> List[str]:
        """Get file content at specific commit."""
        try:
            commit = self.repo.commit(commit_sha)
            file_content = commit.tree[file_path].data_stream.read().decode('utf-8', errors='ignore')
            return file_content.splitlines()
        except:
            return []
    
    @handle_git_errors("Reverse blame")
    def _run_reverse_blame(self, file_path: str, start_sha: str, end_sha: str) -> Optional[Dict[int, Dict[str, Any]]]:
        """Run git blame --reverse and parse output."""
        # Run blame --reverse from affected version to fix commit
        blame_output = self.repo.git.blame(
            '--reverse',
            '--porcelain',
            f'{start_sha}..{end_sha}',
            file_path
        )
        print("git blame", '--reverse', '--porcelain', f'{start_sha}..{end_sha}',file_path)
        return self._parse_reverse_blame_output(blame_output)
    
    def _parse_reverse_blame_output(self, blame_output: str) -> Dict[int, Dict[str, Any]]:
        """Parse git blame --reverse output."""
        blame_data = {}
        lines = blame_output.strip().split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Parse commit line
            if re.match(r'^[0-9a-f]{40}', line):
                parts = line.split()
                if len(parts) >= 3:
                    commit_sha = parts[0]
                    original_line = int(parts[1])  # Line in start commit (affected version)
                    final_line = int(parts[2])      # Line in end commit (fix commit)
                    
                    # Extract metadata
                    author = None
                    author_time = None
                    summary = None
                    
                    # Read metadata lines
                    j = i + 1
                    while j < len(lines) and not lines[j].startswith('\t'):
                        if lines[j].startswith('author '):
                            author = lines[j][7:]
                        elif lines[j].startswith('author-time '):
                            author_time = int(lines[j][12:])
                        elif lines[j].startswith('summary '):
                            summary = lines[j][8:]
                        j += 1
                    
                    # Get the actual line content
                    if j < len(lines) and lines[j].startswith('\t'):
                        line_content = lines[j][1:]
                        
                        blame_data[final_line] = {
                            'commit': commit_sha,
                            'original_line': original_line,
                            'final_line': final_line,
                            'author': author,
                            'author_time': author_time,
                            'date': datetime.fromtimestamp(author_time).isoformat() if author_time else None,
                            'summary': summary,
                            'content': line_content
                        }
                    
                    i = j
                    continue
            
            i += 1
        
        return blame_data
    
    def _map_changes_to_affected_version(self, blame_data: Dict[int, Dict[str, Any]], 
                                       file_changes: Dict[str, Any], file_lines: List[str],
                                       fix_commit_sha: str, parent_commit_sha: str) -> List[Dict[str, Any]]:
        """Map fix commit changes back to lines in affected version."""
        changed_lines = []
        
        # Process each chunk of changes in the fix commit
        for chunk in file_changes.get('chunks', []):
            # For each deleted/modified line in the chunk
            line_offset = 0
            for change in chunk.get('changes', []):
                if change['type'] in ['DELETE', 'MODIFY']:
                    # Calculate line number in parent of fix commit
                    parent_line = chunk['old_start'] + line_offset
                    
                    # Find corresponding line in affected version using blame data
                    for final_line, blame_info in blame_data.items():
                        # Check if this blame entry corresponds to our change
                        if blame_info['commit'] == parent_commit_sha:
                            original_line = blame_info['original_line']
                            
                            # Get content from affected version
                            content = ''
                            if 0 < original_line <= len(file_lines):
                                content = file_lines[original_line - 1]
                            
                            changed_lines.append({
                                'line_number_in_affected_version': original_line,
                                'content_in_affected_version': content,
                                'change_type': change['type'],
                                'changed_by_commit': fix_commit_sha,
                            })
                            break
                
                if change['type'] != 'ADD':
                    line_offset += 1
        
        return changed_lines


class CommitAnalyzer:
    """Analyze fix commits to find changed lines in affected versions."""
    
    def __init__(self, analyzer: GitBlameReverseAnalyzer):
        self.analyzer = analyzer
    
    def analyze_commit(self, commit_data: Dict[str, Any], affected_version: str) -> Optional[Dict[str, Any]]:
        """Analyze a single commit to find which lines in affected version it changes."""
        fix_commit_sha = commit_data['full_sha']
        parent_commit_sha = commit_data['parent_full_sha']

        commit_result = {
            'sha': commit_data['sha'],
            'full_sha': fix_commit_sha,
            'message': commit_data['message'],
            'files': []
        }
        
        for file_data in commit_data.get('files_changed', {}).get('files', []):
            file_result = self._analyze_file(file_data, fix_commit_sha, affected_version, parent_commit_sha)
            if file_result:
                commit_result['files'].append(file_result)
        
        return commit_result if commit_result['files'] else None
    
    def _analyze_file(self, file_data: Dict[str, Any], fix_commit_sha: str,
                     affected_version: str, parent_commit_sha:str) -> Optional[Dict[str, Any]]:
        """Analyze changes in a single file."""
        if file_data.get('change_type') not in ['MODIFY', 'DELETE']:
            return None
        
        file_path = file_data['path']
        
        # Get lines in affected version that were changed by fix commit
        changed_lines = self.analyzer.get_changed_lines_in_affected_version(
            file_path, fix_commit_sha, affected_version, file_data, parent_commit_sha
        )
        
        if not changed_lines:
            return None
        
        return {
            'path': file_path,
            'change_type': file_data.get('change_type'),
            'changed_lines_in_affected_version': changed_lines
        }


def analyze_issue_at_affected_version(repo: git.Repo, issue_data: Dict[str, Any], 
                                     affected_version: str) -> Dict[str, Any]:
    """
    Analyze issue by checking out affected version and using git blame --reverse
    to find which lines were changed by the fix commits.
    """
    analyzer = GitBlameReverseAnalyzer(repo)
    commit_analyzer = CommitAnalyzer(analyzer)
    
    # Store current branch/commit to restore later
    original_ref = repo.head.reference if not repo.head.is_detached else repo.head.commit
    
    try:
        # Checkout the affected version
        if not analyzer.checkout_version(affected_version):
            return {
                'error': f'Could not checkout version {affected_version}',
                'affected_version': affected_version,
                'commits': []
            }
        
        results = {
            'affected_version': affected_version,
            'commits': []
        }
        
        # Analyze each fix commit
        for commit in issue_data.get('commits', []):
            commit_result = commit_analyzer.analyze_commit(commit, affected_version)
            if commit_result:
                results['commits'].append(commit_result)
        
        return results
        
    finally:
        _restore_original_ref(repo, original_ref)


def _restore_original_ref(repo: git.Repo, original_ref):
    """Restore the original branch/commit."""
    try:
        if isinstance(original_ref, git.Head):
            original_ref.checkout()
        else:
            repo.git.checkout(original_ref.hexsha)
    except:
        pass


def process_single_issue(repo: git.Repo, issue_key: str, issue_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Process a single issue and return the analysis result."""
    # Get affected versions
    affected_versions = issue_data.get('issue', {}).get('affects', [])
    if not affected_versions:
        print(f"\n  Warning: No affected versions for {issue_key}")
        return None
    analysis_results = {}
    # Use the first affected version
    for affected_version in affected_versions:
        # Create a temporary clone to work with
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_repo_path = Path(temp_dir) / "repo"
            # Clone the repository to temporary directory
            print(f"\n  Cloning repository for {issue_key}...")
            temp_repo = git.Repo.clone_from(
                str(repo.working_dir),
                str(temp_repo_path),
                no_checkout=False
            )

            # Analyze at affected version
            analysis_result = analyze_issue_at_affected_version(
                temp_repo,
                issue_data,
                affected_version
            )
            analysis_results[affected_version] = analysis_result
    return {
        'issue': issue_data['issue'],
        'blame': analysis_results
    }



def process_issues(input_file: Path, output_file: Path, config: Config) -> None:
    """Process all issues and create test data with blame information."""
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
    
    print(f"Processing {total_issues} issues with git blame --reverse analysis...")
    
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
            
            result = process_single_issue(repo, issue_key, issue_data)
            if result:
                project_results[issue_key] = result
        
        if project_results:
            results[project] = project_results
    
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
        
        for issue_key, issue_result in project_data.items():
            has_lines = False
            for commit in issue_result.get('analysis', {}).get('commits', []):
                for file in commit.get('files', []):
                    lines_count = len(file.get('changed_lines_in_affected_version', []))
                    if lines_count > 0:
                        total_lines_found += lines_count
                        has_lines = True
            if has_lines:
                issues_with_lines += 1
        
        print(f"  - {project}: {len(project_data)} issues analyzed, "
              f"{issues_with_lines} with changed lines found, "
              f"{total_lines_found} total line numbers identified in affected versions")


def main():
    """Main function."""
    config = Config()
    
    input_file = config.outputs_dir / "p4_issues_with_deleted_chunks.json"
    output_file = config.outputs_dir / "p7_test_data_with_blame.json"
    
    if not input_file.exists():
        print(f"Error: Input file {input_file} not found!")
        print("Please run p4_filter_issues_with_deleted_chunk.py first.")
        sys.exit(1)
    
    process_issues(input_file, output_file, config)


if __name__ == "__main__":
    main()