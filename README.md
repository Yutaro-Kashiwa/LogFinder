# LogFinder

A comprehensive tool for analyzing JIRA issues with log attachments and finding related Git commits.
## Preparation
- Download issues from JIRA
https://issues.apache.org/jira/browse/HBASE-29041?jql=project%20%3D%20HBASE%20AND%20issuetype%20%3D%20Bug%20AND%20priority%20in%20(Minor%2C%20Trivial)%20AND%20affectedVersion%20in%20releasedVersions()%20AND%20fixVersion%20in%20releasedVersions()


## Overview

LogFinder processes JIRA CSV exports to:
1. Extract and structure issue data from CSV files
2. Filter issues that have log file attachments
3. Search Git repositories for commits that fix these issues
4. Filter issues based on commit change patterns (e.g., only additions)
5. Download log files from JIRA attachments
6. Export results to CSV format for analysis

## Quick Start

```bash
# Run the complete pipeline
python logfinder.py

# Or run individual steps
python logfinder.py process  # Process CSV files
python logfinder.py filter   # Filter log issues
python logfinder.py search   # Search for commits
python logfinder.py export   # Export to CSV
```

## Pipeline Scripts

The project includes several Python scripts that form a data processing pipeline:

1. **p1_json_creator_from_jira.py** - Converts JIRA CSV exports to structured JSON
2. **p2_find_log_attachments.py** - Filters issues that have log attachments
3. **p3_search_fix_commits.py** - Searches Git repositories for fix commits
4. **p4_filter_added_only_issues.py** - Filters issues with only ADDED changes
5. **p5_download_log_files.py** - Downloads log files from JIRA attachments
6. **p9_export_to_csv.py** - Exports results to CSV format

## Directory Structure

```
LogFinder/
├── logfinder.py              # Main CLI script
├── src/                      # Source modules
│   ├── utils.py             # Shared utilities and configuration
│   ├── csv_processor.py     # CSV processing
│   ├── log_filter.py        # Log attachment filtering
│   └── commit_finder.py     # Git commit searching
├── inputs/                   # Input data
│   └── logs/                # JIRA CSV export files
├── outputs/                  # Generated output files (excluded from git)
├── repos/                    # Cloned Git repositories (excluded from git)
├── downloads/                # Downloaded log files (excluded from git)
│   └── logs/                # Organized by project/issue
└── p*.py                     # Individual pipeline scripts
```

## Input Requirements

Place JIRA CSV export files in the `inputs/logs/` directory. The tool expects CSV files with these columns:
- Issue key
- Summary  
- Project name
- Status
- Priority
- Created
- Affects Version/s
- Attachment (format: date;username;filename;url)

## Output Files

The tool generates several output files in the `outputs/` directory:

- **p1_issues.json** - All processed issues from CSV
- **p2_issues_with_logs.json** - Issues with log attachments
- **p2_project_statistics.json** - Statistics by project
- **p3_issues_with_fix_commits.json** - Issues with associated commits
- **p4_issues_with_added_only.json** - Issues with only ADDED changes
- **p9_issues_with_commits_[PROJECT].csv** - CSV export per project
- **p9_summary_by_project.csv** - Summary statistics

Downloaded log files are organized in `downloads/logs/[Project]/[Issue]/`

## Supported Projects

Currently configured for:
- Apache HBase
- Apache ZooKeeper

Additional projects can be added by updating the repository configuration in `src/utils.py`.

## Configuration

Repository configurations and paths are managed in `src/utils.py`. The `Config` class contains:
- Repository URLs and local paths
- Input/output directory paths  
- Processing settings

## Requirements

```bash
# Install required packages
pip install GitPython requests
```

- Python 3.7+
- GitPython library
- requests library
- Git command line tool
- Internet connection for repository cloning and file downloads

## Usage Examples

### Using the Main CLI

```bash
# Get help
python logfinder.py --help

# Run with verbose output
python logfinder.py --verbose

# Run specific steps
python logfinder.py process
python logfinder.py search
```

### Using Individual Scripts

```bash
# 1. Process JIRA CSV files
python p1_json_creator_from_jira.py

# 2. Find issues with log attachments
python p2_find_log_attachments.py

# 3. Search for fix commits
python p3_search_fix_commits.py

# 4. Filter issues with only additions
python p4_filter_added_only_issues.py

# 5. Download log files
python p5_download_log_files.py

# 6. Export to CSV
python p9_export_to_csv.py
```

## Notes

- Large directories (downloads/, repos/, outputs/) are excluded from git via .gitignore
- JIRA attachments may require authentication for download
- Git operations can take time for large repositories
- The tool caches cloned repositories for faster subsequent runs

## License

This project is for research and analysis purposes.