#!/usr/bin/env python3
"""
Git command wrapper to replace GitPython functionality.
This module provides a simple interface to git commands using subprocess.
"""
import subprocess
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import tempfile
import shutil


class GitError(Exception):
    """Custom exception for git command errors."""
    pass


class GitRepo:
    """A simple git repository wrapper using git commands."""
    
    def __init__(self, path: str):
        """Initialize repository at given path."""
        self.working_dir = str(Path(path).absolute())
        if not self._is_git_repo():
            raise GitError(f"Not a git repository: {self.working_dir}")
    
    def _is_git_repo(self) -> bool:
        """Check if the path is a git repository."""
        try:
            self._run_git_command(['rev-parse', '--git-dir'])
            return True
        except GitError:
            return False
    
    def _run_git_command(self, args: List[str], check: bool = True) -> subprocess.CompletedProcess:
        """Run a git command in the repository directory."""
        cmd = ['git', '-C', self.working_dir] + args
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if check and result.returncode != 0:
            raise GitError(f"Git command failed: {' '.join(cmd)}\n{result.stderr}")
        
        return result
    
    def get_remote_url(self, remote: str = 'origin') -> Optional[str]:
        """Get the URL of a remote."""
        try:
            result = self._run_git_command(['remote', 'get-url', remote])
            return result.stdout.strip()
        except GitError:
            return None
    
    def get_tags(self) -> List[str]:
        """Get all tags in the repository."""
        result = self._run_git_command(['tag', '-l'])
        return [tag for tag in result.stdout.strip().split('\n') if tag]
    
    def get_branches(self) -> List[str]:
        """Get all local branches."""
        result = self._run_git_command(['branch', '--format=%(refname:short)'])
        return [branch for branch in result.stdout.strip().split('\n') if branch]
    
    def checkout(self, ref: str) -> bool:
        """Checkout a specific commit, branch, or tag."""
        try:
            self._run_git_command(['checkout', ref])
            return True
        except GitError:
            return False
    
    def rev_parse(self, ref: str) -> Optional[str]:
        """Resolve a reference to a commit SHA."""
        try:
            result = self._run_git_command(['rev-parse', ref])
            return result.stdout.strip()
        except GitError:
            return None
    
    def diff(self, ref1: str, ref2: str, *args) -> str:
        """Run git diff between two references."""
        cmd_args = ['diff', ref1, ref2]
        cmd_args.extend(args)
        result = self._run_git_command(cmd_args)
        return result.stdout
    
    def pull(self, remote: str = 'origin', branch: Optional[str] = None) -> bool:
        """Pull from remote."""
        try:
            args = ['pull', remote]
            if branch:
                args.append(branch)
            self._run_git_command(args)
            return True
        except GitError:
            return False
    
    def get_commit_info(self, ref: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific commit."""
        try:
            # Get basic commit info
            format_str = '%H%x00%an%x00%ae%x00%at%x00%cn%x00%ce%x00%ct%x00%P%x00%s%x00%b'
            result = self._run_git_command(['show', '-s', f'--format={format_str}', ref])
            parts = result.stdout.strip().split('\x00')
            
            if len(parts) < 10:
                return None
            
            return {
                'hexsha': parts[0],
                'author_name': parts[1],
                'author_email': parts[2],
                'author_time': int(parts[3]),
                'committer_name': parts[4],
                'committer_email': parts[5],
                'committer_time': int(parts[6]),
                'parents': parts[7].split() if parts[7] else [],
                'subject': parts[8],
                'body': parts[9] if len(parts) > 9 else ''
            }
        except (GitError, ValueError, IndexError):
            return None
    
    def get_commit_stats(self, ref: str) -> Dict[str, int]:
        """Get statistics for a commit."""
        try:
            result = self._run_git_command(['show', '--stat', '--format=', ref])
            lines = result.stdout.strip().split('\n')
            
            stats = {'insertions': 0, 'deletions': 0, 'files': 0}
            
            # Parse the last line which contains the summary
            for line in reversed(lines):
                if 'changed' in line:
                    parts = line.strip().split(',')
                    for part in parts:
                        if 'file' in part:
                            stats['files'] = int(part.strip().split()[0])
                        elif 'insertion' in part:
                            stats['insertions'] = int(part.strip().split()[0])
                        elif 'deletion' in part:
                            stats['deletions'] = int(part.strip().split()[0])
                    break
            
            return stats
        except (GitError, ValueError):
            return {'insertions': 0, 'deletions': 0, 'files': 0}
    
    def get_commit_diff_files(self, ref: str) -> List[Dict[str, Any]]:
        """Get list of files changed in a commit."""
        try:
            # Get file changes with rename detection
            result = self._run_git_command(['show', '--name-status', '--format=', '-M', ref])
            
            files = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                    
                parts = line.split('\t')
                if len(parts) >= 2:
                    status = parts[0]
                    if status.startswith('R'):
                        # Rename
                        old_path = parts[1]
                        new_path = parts[2] if len(parts) > 2 else parts[1]
                        files.append({
                            'change_type': 'RENAME',
                            'path': new_path,
                            'old_path': old_path
                        })
                    else:
                        change_map = {
                            'A': 'ADD',
                            'M': 'MODIFY',
                            'D': 'DELETE'
                        }
                        files.append({
                            'change_type': change_map.get(status, 'MODIFY'),
                            'path': parts[1]
                        })
            
            return files
        except GitError:
            return []
    
    def get_references(self) -> List[str]:
        """Get all references in the repository."""
        try:
            result = self._run_git_command(['for-each-ref', '--format=%(refname)'])
            return [ref for ref in result.stdout.strip().split('\n') if ref and 'origin' not in ref]
        except GitError:
            return []
    
    def get_commit_diff_details(self, ref: str) -> Dict[str, Any]:
        """Get detailed diff information for a commit including patches."""
        try:
            # First get the basic stats
            stats = self.get_commit_stats(ref)
            files_changed = {
                "total_files": stats['files'],
                "total_insertions": stats['insertions'],
                "total_deletions": stats['deletions'],
                "files": []
            }
            
            # Get detailed diff with patches
            parent = self.get_commit_info(ref)
            if parent and parent['parents']:
                parent_sha = parent['parents'][0]
                result = self._run_git_command(['diff', parent_sha, ref, '--no-prefix', "-w"])
            else:
                # Initial commit
                result = self._run_git_command(['show', ref, '--no-prefix', "-w"])
            
            # Parse the diff output
            current_file = None
            current_chunks = []
            diff_lines = result.stdout.split('\n')
            
            i = 0
            while i < len(diff_lines):
                line = diff_lines[i]
                
                if line.startswith('diff --git'):
                    # Save previous file if exists
                    if current_file:
                        files_changed["files"].append(current_file)
                    
                    # Start new file
                    parts = line.split()
                    if len(parts) >= 4:
                        file_path = parts[3] if parts[3] != parts[2] else parts[2]
                        file_path = file_path.lstrip('b/').lstrip('a/')
                        
                        # Determine change type from the next few lines
                        change_type = "MODIFY"
                        if i + 1 < len(diff_lines):
                            next_line = diff_lines[i + 1]
                            if next_line.startswith('new file'):
                                change_type = "ADD"
                            elif next_line.startswith('deleted file'):
                                change_type = "DELETE"
                            elif next_line.startswith('rename from'):
                                change_type = "RENAME"
                        
                        # Get file stats from our stats dict
                        file_stats = self._get_file_stats_from_diff(ref, file_path)
                        
                        current_file = {
                            "path": file_path,
                            "change_type": change_type,
                            "insertions": file_stats.get('insertions', 0),
                            "deletions": file_stats.get('deletions', 0),
                            "lines_changed": file_stats.get('insertions', 0) + file_stats.get('deletions', 0),
                            "chunks": []
                        }
                        current_chunks = []
                
                elif line.startswith('@@') and current_file:
                    # Parse chunk header
                    chunk = self._parse_chunk_header(line)
                    if chunk:
                        current_chunks.append(chunk)
                        current_file["chunks"].append(chunk)
                
                elif current_chunks and len(line) > 0 and line[0] in ['+', '-', ' ']:
                    # Add change to current chunk
                    if line[0] != ' ' and current_chunks:
                        current_chunk = current_chunks[-1]
                        change_type = "ADD" if line[0] == '+' else "DELETE"
                        current_chunk["changes"].append({
                            "line_number": current_chunk.get("start_line", 0) + len(current_chunk["changes"]),
                            "type": change_type,
                            "content": line[1:] if len(line) > 1 else ""
                        })
                
                i += 1
            
            # Don't forget the last file
            if current_file:
                files_changed["files"].append(current_file)
            
            # Sort files by most lines changed
            files_changed["files"].sort(key=lambda x: x["lines_changed"], reverse=True)
            
            return files_changed
            
        except Exception as e:
            return {
                "total_files": 0,
                "total_insertions": 0,
                "total_deletions": 0,
                "files": [],
                "error": f"Could not retrieve file changes: {str(e)}"
            }
    
    def _get_file_stats_from_diff(self, ref: str, file_path: str) -> Dict[str, int]:
        """Get insertion/deletion stats for a specific file in a commit."""
        try:
            result = self._run_git_command(['show', '--numstat', '--format=', ref, '--', file_path])
            lines = result.stdout.strip().split('\n')
            
            for line in lines:
                if line:
                    parts = line.split('\t')
                    if len(parts) >= 3:
                        insertions = int(parts[0]) if parts[0] != '-' else 0
                        deletions = int(parts[1]) if parts[1] != '-' else 0
                        return {'insertions': insertions, 'deletions': deletions}
            
            return {'insertions': 0, 'deletions': 0}
        except:
            return {'insertions': 0, 'deletions': 0}
    
    def _parse_chunk_header(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a diff chunk header line."""
        import re
        match = re.match(r'@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@', line)
        if match:
            return {
                "old_start": int(match.group(1)),
                "old_count": int(match.group(2)) if match.group(2) else 1,
                "new_start": int(match.group(3)),
                "new_count": int(match.group(4)) if match.group(4) else 1,
                "start_line": int(match.group(1)),
                "changes": []
            }
        return None
    
    def iter_commits(self, ref: str = 'HEAD', max_count: Optional[int] = None) -> List[str]:
        """Iterate through commits reachable from ref."""
        try:
            args = ['rev-list', ref]
            if max_count:
                args.extend(['--max-count', str(max_count)])
            
            result = self._run_git_command(args)
            return [sha for sha in result.stdout.strip().split('\n') if sha]
        except GitError:
            return []
    
    def search_commits(self, grep: Optional[str] = None, path: Optional[str] = None,
                      since: Optional[str] = None, until: Optional[str] = None,
                      max_count: Optional[int] = None) -> List[str]:
        """Search for commits matching criteria."""
        args = ['log', '--format=%H']
        
        if grep:
            args.extend(['--grep', grep])
        if path:
            args.extend(['--', path])
        if since:
            args.extend(['--since', since])
        if until:
            args.extend(['--until', until])
        if max_count:
            args.extend(['--max-count', str(max_count)])
        
        try:
            result = self._run_git_command(args)
            return [sha for sha in result.stdout.strip().split('\n') if sha]
        except GitError:
            return []
    
    @staticmethod
    def clone_from(url: str, to_path: str, **kwargs) -> 'GitRepo':
        """Clone a repository from URL to path."""
        cmd = ['git', 'clone', url, to_path]
        
        # Add any additional arguments
        if kwargs.get('no_checkout'):
            cmd.append('--no-checkout')
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise GitError(f"Failed to clone repository: {result.stderr}")
        
        return GitRepo(to_path)