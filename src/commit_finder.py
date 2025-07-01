#!/usr/bin/env python3
"""
Git commit finder module for LogFinder.
Searches for commits that fix issues in Git repositories.
"""
import git
import re
from pathlib import Path
from typing import Dict, List
from collections import defaultdict
from .utils import Config, load_json, save_json, print_progress


class CommitFinder:
    """Find Git commits that reference specific issues."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def search_fix_commits(self, input_file: Path = None) -> Path:
        """Search for commits that fix issues with log attachments."""
        if input_file is None:
            input_file = self.config.outputs_dir / "issues_with_logs.json"
        
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        issues = load_json(input_file)
        
        # Setup repositories
        active_repos = self._setup_repositories()
        if not active_repos:
            raise RuntimeError("No repositories available for searching")
        
        # Group issues by project
        issues_by_project = self._group_issues_by_project(issues)
        
        # Search for commits in each repository
        results = {}
        for project, project_issues in issues_by_project.items():
            if project in active_repos:
                print(f"\nSearching for fix commits in {project} repository...")
                results[project] = self._search_project_commits(
                    project, project_issues, active_repos[project]
                )
            else:
                print(f"Warning: No repository available for project {project}")
        
        # Save results - both combined and by project
        output_file = self.config.outputs_dir / "p3_issues_with_fix_commits.json"
        save_json(results, output_file)
        print(f"\nResults saved to: {output_file}")
        
        # Save separate files by project
        # project_files = []
        # for project, project_data in results.items():
        #     project_file = self.config.outputs_dir / f"issues_with_fix_commits_{project.replace(' ', '_')}.json"
        #     save_json({project: project_data}, project_file)
        #     project_files.append(project_file)
        #     print(f"Project results saved to: {project_file}")
        
        # Print summary
        self._print_search_summary(results)
        
        return output_file
    
    def _setup_repositories(self) -> Dict[str, git.Repo]:
        """Clone or update repositories as needed."""
        print("Setting up repositories...")
        active_repos = {}
        
        for project, repo_info in self.config.repositories.items():
            print(f"\n{project}:")
            repo = self._clone_or_update_repo(repo_info["url"], repo_info["local_path"])
            if repo:
                active_repos[project] = repo
            else:
                print(f"  Skipping {project} due to errors")
        
        return active_repos
    
    def _clone_or_update_repo(self, repo_url: str, local_path: str) -> git.Repo:
        """Clone repository or update if it already exists."""
        local_path_obj = Path(local_path)
        
        if local_path_obj.exists() and (local_path_obj / ".git").exists():
            print(f"  Repository already exists at {local_path}, pulling latest changes...")
            try:
                repo = git.Repo(local_path)
                origin = repo.remotes.origin
                origin.pull()
                print(f"  Updated successfully")
                return repo
            except Exception as e:
                print(f"  Warning: Could not update repository: {e}")
                return git.Repo(local_path)
        else:
            print(f"  Cloning repository from {repo_url}...")
            print(f"  This may take a few minutes...")
            try:
                repo = git.Repo.clone_from(repo_url, local_path)
                print(f"  Cloned successfully")
                return repo
            except Exception as e:
                print(f"  Error cloning repository: {e}")
                return None
    
    def _group_issues_by_project(self, issues: List[Dict]) -> Dict[str, List[Dict]]:
        """Group issues by project name."""
        issues_by_project = defaultdict(list)
        for issue in issues:
            project = issue["Project name"]
            issues_by_project[project].append(issue)
        return dict(issues_by_project)
    
    def _search_project_commits(self, project: str, issues: List[Dict], repo: git.Repo) -> Dict:
        """Search for commits in a specific project repository - optimized version."""
        github_url = self.config.repositories[project]["github_url"]
        
        # Display available branches
        try:
            branches = [ref.name for ref in repo.references if 'origin' not in ref.name]
            print(f"Available branches: {', '.join(branches[:5])}{'...' if len(branches) > 5 else ''}")
        except:
            raise
        
        # Initialize results structure
        project_results = {}
        for issue in issues:
            issue_key = issue["Issue key"]
            project_results[issue_key] = {
                "issue": {
                    "key": issue_key,
                    "summary": issue["Summary"],
                    "status": issue["Status"],
                    "priority": issue["Priority"],
                    "created": issue["Created"],
                    "affects": issue["Affects Version/s"],
                    "log": issue["Log attachments"]
                },
                "commits": [],
                "commit_count": 0
            }
        
        # Create a map of issue keys to patterns for efficient matching
        issue_patterns = {}
        for issue in issues:
            issue_key = issue["Issue key"]
            issue_patterns[issue_key] = [
                re.compile(rf'^{re.escape(issue_key)}\b', re.IGNORECASE | re.MULTILINE),
                re.compile(rf'^#{re.escape(issue_key)}\b', re.IGNORECASE | re.MULTILINE),
                re.compile(rf'^\[{re.escape(issue_key)}]', re.IGNORECASE | re.MULTILINE),
            ]
            # TODO: DRUIDの時は()のみhttps://github.com/apache/druid/commits/master/
        
        # Search commits once and match against all issues
        print(f"Scanning repository commits across all branches...")
        commits_processed = 0
        commits_with_matches = 0
        seen_commits = set()  # Track commits we've already processed
        
        # Get prioritized branches (limit to important ones for efficiency)
        try:
            all_branches = [ref.name for ref in repo.references if 'origin' not in ref.name]
        except:
            raise
        
        # Scan each selected branch
        for branch_idx, branch in enumerate(all_branches):
            try:
                print(f"  [{branch_idx + 1}/{len(all_branches)}] Scanning branch: {branch}")
                branch_new_commits = 0
                
                for commit in repo.iter_commits(branch, max_count=50000):
                    # Skip if we've already processed this commit
                    if commit.hexsha in seen_commits:
                        continue

                    seen_commits.add(commit.hexsha)
                    commits_processed += 1
                    branch_new_commits += 1
                    if len(commit.parents) >= 2:
                        continue

                    if branch_new_commits % 5000 == 0:
                        print(f"    Processed {branch_new_commits} new commits in this branch...")
                    
                    commit_msg = commit.message
                    matched_issues = set()
                    
                    # Check this commit against all issue patterns
                    for issue_key, patterns in issue_patterns.items():
                        for pattern in patterns:
                            if pattern.search(commit_msg):
                                matched_issues.add(issue_key)
                                break
                    
                    # If we found matches, add commit info to those issues
                    if matched_issues:
                        commits_with_matches += 1
                        commit_info = {
                            "sha": commit.hexsha[:8],
                            "full_sha": commit.hexsha,
                            "num_parents": len(commit.parents),
                            "parent_full_sha": commit.parents[0].hexsha,
                            "author": str(commit.author),
                            "author_email": commit.author.email,
                            "date": commit.committed_datetime.isoformat(),
                            "message": self._truncate_message(commit.message.strip()),
                            "branch": branch,
                            "github_url": f"{github_url}/commit/{commit.hexsha}",
                            "files_changed": self._get_commit_file_changes(commit)
                        }
                        
                        for issue_key in matched_issues:
                            # Limit to 5 commits per issue
                            if len(project_results[issue_key]['commits']) < 5:
                                project_results[issue_key]['commits'].append(commit_info)
                                project_results[issue_key]['commit_count'] += 1
                
                print(f"    Found {branch_new_commits} new commits in branch {branch}")
                
            except Exception as e:
                print(f"  Warning: Could not scan branch {branch}: {str(e)[:100]}")
                continue
        
        print(f"\nScanned {commits_processed} commits, found {commits_with_matches} with issue references")
        
        # Print summary of results
        issues_with_commits = sum(1 for r in project_results.values() if r['commit_count'] > 0)
        total_commit_refs = sum(r['commit_count'] for r in project_results.values())
        print(f"Matched {issues_with_commits}/{len(issues)} issues with {total_commit_refs} total commit references")
        
        return project_results
    
    
    def _get_commit_file_changes(self, commit: git.Commit) -> Dict:
        """Extract detailed file changes information from a commit."""
        try:
            # Get commit statistics
            stats = commit.stats
            files_changed = {
                "total_files": stats.total['files'],
                "total_insertions": stats.total['insertions'], 
                "total_deletions": stats.total['deletions'],
                "files": []
            }
            
            # Get detailed file changes with diff information
            try:
                # Get the diff for this commit
                if commit.parents:
                    # Compare with first parent
                    diff = commit.parents[0].diff(commit, create_patch=True)
                else:
                    # Initial commit - compare with empty tree
                    diff = commit.diff(git.NULL_TREE, create_patch=True)
                
                # Process each changed file
                for diff_item in diff:
                    file_path = diff_item.b_path or diff_item.a_path
                    if not file_path:
                        continue
                    # Determine change type
                    change_type = self._get_change_type(diff_item)
                    # Get basic stats
                    file_stats = stats.files.get(file_path, {'insertions': 0, 'deletions': 0})
                    
                    file_info = {
                        "path": file_path,
                        "change_type": change_type,
                        "insertions": file_stats['insertions'],
                        "deletions": file_stats['deletions'],
                        "lines_changed": file_stats['insertions'] + file_stats['deletions'],
                        "chunks": []
                    }
                    
                    # Parse diff for detailed chunk information
                    if hasattr(diff_item, 'diff') and diff_item.diff:
                        chunks = self._parse_diff_chunks(diff_item.diff.decode('utf-8', errors='ignore'))
                        file_info["chunks"] = chunks
                    
                    files_changed["files"].append(file_info)
                
            except Exception as e:
                raise
                # Fallback to basic stats if diff parsing fails
                print(f"    Warning: Could not parse diff for commit {commit.hexsha[:8]}: {str(e)[:50]}")
                for file_path, file_stats in stats.files.items():
                    file_info = {
                        "path": file_path,
                        "change_type": "MODIFY",  # Default assumption
                        "insertions": file_stats['insertions'],
                        "deletions": file_stats['deletions'],
                        "lines_changed": file_stats['insertions'] + file_stats['deletions'],
                        "chunks": []
                    }
                    files_changed["files"].append(file_info)
            
            # Sort files by most lines changed
            files_changed["files"].sort(key=lambda x: x["lines_changed"], reverse=True)
            
            return files_changed
            
        except Exception as e:
            # Fallback if stats are not available
            return {
                "total_files": 0,
                "total_insertions": 0,
                "total_deletions": 0,
                "files": [],
                "error": f"Could not retrieve file changes: {str(e)}"
            }
    
    def _get_change_type(self, diff_item) -> str:
        """Determine the type of change for a file."""
        if diff_item.new_file:
            return "ADD"
        elif diff_item.deleted_file:
            return "DELETE"
        elif diff_item.renamed_file:
            return "RENAME"
        else:
            return "MODIFY"
    
    def _parse_diff_chunks(self, diff_text: str) -> List[Dict]:
        """Parse diff text to extract detailed chunk information."""
        chunks = []
        lines = diff_text.split('\n')
        
        current_chunk = None
        line_no = None
        for line in lines:
            # Look for chunk headers (@@)
            if line.startswith('@@'):
                if current_chunk:
                    chunks.append(current_chunk)
                
                # Parse chunk header: @@ -old_start,old_count +new_start,new_count @@
                try:
                    import re
                    match = re.match(r'@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@', line)
                    if match:
                        old_start = int(match.group(1))
                        old_count = int(match.group(2)) if match.group(2) else 1
                        new_start = int(match.group(3))
                        new_count = int(match.group(4)) if match.group(4) else 1
                        line_no = old_start
                        current_chunk = {
                            "old_start": old_start,
                            "old_count": old_count,
                            "new_start": new_start,
                            "new_count": new_count,
                            "changes": []
                        }
                except Exception:
                    # Skip malformed chunk headers
                    continue
                    
            elif current_chunk and (line.startswith('+') or line.startswith('-') or line.startswith(' ')):
                # Parse change lines
                change_type = "CONTEXT"
                if line.startswith('+'):
                    change_type = "ADD"
                elif line.startswith('-'):
                    change_type = "DELETE"

                if change_type != "CONTEXT":
                    current_chunk["changes"].append({
                        "line_number": line_no,
                        "type": change_type,
                        "content": line[1:] if len(line) > 1 else ""  # Remove +/- prefix
                    })
                line_no += 1
        
        # Add the last chunk
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks
    
    def _truncate_message(self, message: str, max_length: int = 300) -> str:
        """Truncate commit message if too long."""
        if len(message) > max_length:
            return message[:max_length] + "..."
        return message
    
    def _print_search_summary(self, results: Dict) -> None:
        """Print summary of search results."""
        print("\nSummary:")
        for project, project_results in results.items():
            total_issues = len(project_results)
            issues_with_commits = sum(1 for r in project_results.values() if r['commit_count'] > 0)
            total_commits = sum(r['commit_count'] for r in project_results.values())
            
            print(f"\n{project}:")
            print(f"  - Total issues searched: {total_issues}")
            print(f"  - Issues with commits found: {issues_with_commits}")
            print(f"  - Total commits found: {total_commits}")
            
            # Show examples
            if issues_with_commits > 0:
                print(f"\n  Example issues with commits:")
                examples = [k for k, v in project_results.items() if v['commit_count'] > 0][:3]
                for ex in examples:
                    print(f"    - {ex}: {project_results[ex]['commit_count']} commits")