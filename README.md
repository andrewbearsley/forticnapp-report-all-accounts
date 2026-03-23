# Lacework Compliance Report - All Accounts

This project provides a Python script to retrieve a combined compliance report for all accounts (AWS, Azure, or GCP) configured in your Lacework instance. Instead of manually requesting reports for each account, the script automatically discovers all accounts and fetches the compliance reports for each one.

## Prerequisites

- Lacework CLI installed and configured
- Python 3.6 or higher
- Lacework API key file in JSON format (see API Key Format below)

## Installation

### Option 1: Using a Virtual Environment (Recommended)

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd forticnapp-report-all-accounts
   ```

2. Create and activate a virtual environment:
   ```bash
   # On macOS/Linux
   python3 -m venv venv
   source venv/bin/activate
   
   # On Windows
   python -m venv venv
   venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Make the script executable (optional, for direct execution):
   ```bash
   chmod +x scripts/forticnapp_get_consolidated_report.py
   ```

### Option 2: Global Installation

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd forticnapp-report-all-accounts
   ```

2. Install dependencies:
   ```bash
   pip3 install -r requirements.txt
   ```

3. Make the script executable (optional):
   ```bash
   chmod +x scripts/forticnapp_get_consolidated_report.py
   ```

### Verify Installation

Verify that the script works:
```bash
python3 scripts/forticnapp_get_consolidated_report.py --help
```

### Dependencies

The project requires the following Python packages (automatically installed via `requirements.txt`):

- **openpyxl** (>=3.1.0): For generating Excel spreadsheets

All other dependencies are part of Python's standard library.

### Deactivating Virtual Environment

When you're done working with the project, you can deactivate the virtual environment:
```bash
deactivate
```

## API Key Format

Your API key file should be a JSON file with the following structure:

```json
{
  "keyId": "YOUR_KEY_ID",
  "secret": "YOUR_SECRET",
  "account": "your-account.lacework.net",
  "subAccount": "optional-subaccount"
}
```

An example template is provided at `api-key/api-key.example.json`. To use it:

1. Copy the example file: `cp api-key/api-key.example.json api-key/my-api-key.json`
2. Edit `api-key/my-api-key.json` with your actual credentials

**Note**: The `subAccount` field is optional - only include it if you're using a sub-account in your Lacework instance.

## Finding Report Names

To find available compliance report names, use the Lacework CLI:

```bash
lacework report-definitions list
```

This will show all available compliance reports with their names and types (AWS, Azure, or GCP).

**Note**: The `report-definitions list` command may not show recently-added custom frameworks due to a known issue with the `/api/v2/ReportDefinitions` endpoint. For custom frameworks, use the `--cloud-type` flag to specify the cloud provider explicitly (see Options below).

## Usage

### Basic Syntax

```bash
python3 scripts/forticnapp_get_consolidated_report.py <api-key-path> <report-name> [options]
```

### Arguments

- `api-key-path`: Path to your Lacework API key JSON file
- `report-name`: Name of the compliance report to fetch

### Options

- `-v, --verbose`: Enable verbose output for debugging
- `--cloud-type {aws,azure,gcp}`: Cloud type override. Required for custom frameworks not listed in `report-definitions list`
- `--use-cache`: Use cached account list data (useful for testing without making API calls)
- `--no-concatenate`: Skip concatenation (reports will only be saved as individual JSON files in `output/`)
- `--keep-intermediate`: Keep intermediate JSON files in `output/` directory after concatenation (default: files are cleaned up automatically)
- `-f, --format FORMAT`: Output format for concatenation: `json` or `excel` (default: `excel`)
- `-o, --output FILE`: Output file path for concatenated report (default: `forticnapp-compliance-report.xlsx` or `.json`)
- `--include-compliant`: Include all statuses in Excel output (default: only NonCompliant rows are shown)
- `--skip-tags`: Skip fetching resource tags from the inventory API (tags column will be empty)
- `--test`: Test mode — limit to first 3 accounts for quick validation

### Examples

#### AWS CIS Benchmark

```bash
# Basic usage: fetch and concatenate to Excel (creates forticnapp-compliance-report.xlsx)
python3 scripts/forticnapp_get_consolidated_report.py api-key/my-api-key.json "CIS Amazon Web Services Foundations Benchmark v1.4.0"

# Fetch and concatenate to JSON
python3 scripts/forticnapp_get_consolidated_report.py api-key/my-api-key.json "CIS Amazon Web Services Foundations Benchmark v1.4.0" -f json

# With custom output filename
python3 scripts/forticnapp_get_consolidated_report.py api-key/my-api-key.json "CIS Amazon Web Services Foundations Benchmark v1.4.0" -o my-report.xlsx

# Skip concatenation (only save individual JSON files in output/)
python3 scripts/forticnapp_get_consolidated_report.py api-key/my-api-key.json "CIS Amazon Web Services Foundations Benchmark v1.4.0" --no-concatenate

# Keep intermediate JSON files after concatenation (default: files are cleaned up)
python3 scripts/forticnapp_get_consolidated_report.py api-key/my-api-key.json "CIS Amazon Web Services Foundations Benchmark v1.4.0" --keep-intermediate

# With verbose output
python3 scripts/forticnapp_get_consolidated_report.py api-key/my-api-key.json "CIS Amazon Web Services Foundations Benchmark v1.4.0" -v
```

#### Custom Frameworks

```bash
# Custom frameworks require --cloud-type since report-definitions may not list them
python3 scripts/forticnapp_get_consolidated_report.py api-key/my-api-key.json "My Custom AWS Framework" --cloud-type aws

# Custom Azure framework
python3 scripts/forticnapp_get_consolidated_report.py api-key/my-api-key.json "My Custom Azure Framework" --cloud-type azure
```

#### Azure CIS Benchmark

```bash
# Basic usage: fetch and concatenate
python3 scripts/forticnapp_get_consolidated_report.py api-key/my-api-key.json "CIS Microsoft Azure Foundations Benchmark v1.5.0"

# With verbose output
python3 scripts/forticnapp_get_consolidated_report.py api-key/my-api-key.json "CIS Microsoft Azure Foundations Benchmark v1.5.0" -v
```

#### GCP CIS Benchmark

```bash
# Basic usage: fetch and concatenate
python3 scripts/forticnapp_get_consolidated_report.py api-key/my-api-key.json "CIS Google Cloud Platform Foundation Benchmark v2.0.0"

# With verbose output and caching
python3 scripts/forticnapp_get_consolidated_report.py api-key/my-api-key.json "CIS Google Cloud Platform Foundation Benchmark v2.0.0" --use-cache -v
```

### Report Name Format

Report names must match exactly as configured in your Lacework instance. Common CIS report names include:

- **AWS**: `"CIS Amazon Web Services Foundations Benchmark v1.4.0"`
- **Azure**: `"CIS Microsoft Azure Foundations Benchmark v1.5.0"` or `"Azure CIS Benchmark"`
- **GCP**: `"CIS Google Cloud Platform Foundation Benchmark v2.0.0"` or `"GCP CIS Benchmark"`

**Note**: Use quotes around report names that contain spaces or special characters.

## Output

**Excel Format** (default):
- Creates a spreadsheet with two sheets:
  - **Summary**: Overview metrics and non-compliant policies table sorted by severity
  - **Recommendations**: One row per violation, with columns: Section, Policy, Severity, Account, Account Name, Status, Resource, First Seen, Remediation, Docs, Tags
- Features:
  - Each violation expanded to its own row with the individual resource identifier
  - Account Name resolved from the cloud provider (e.g., AWS account alias)
  - First Seen date tracked via local CSV history (see Violation History below)
  - Remediation steps fetched from the Policies API
  - Docs column with clickable links to Fortinet policy documentation
  - Resource tags fetched from the inventory API (use `--skip-tags` to disable)
  - Default: only NonCompliant rows shown (use `--include-compliant` for all statuses)
  - Auto-filters on all columns
  - Severity labels (Critical, High, Medium, Low, Info)

**JSON Format**:
- Creates a single JSON file matching the structure of individual account reports
- Contains:
  - `reportTitle`, `reportType`, `reportTime`
  - `recommendations`: Array of all recommendations from all accounts
  - `summary`: Single summary object with aggregated statistics

### Violation History (First Seen)

The script tracks when each violation was first detected using a local CSV history file stored in the `history/` directory. Each report gets its own history file (e.g., `history/My Custom AWS Framework.csv`).

- **First run**: All violations are recorded with today's date
- **Subsequent runs**: Existing violations keep their original first-seen date; only new violations get the current date
- The history file is keyed on `(policy_id, account_id, resource)` — so the same misconfiguration on the same resource in the same account is tracked as one entry
- History files are local and not committed to git — each environment maintains its own history


## References

- CLI Documentation: [Get started with Lacework FortiCNAPP CLI](https://docs.fortinet.com/document/lacework-forticnapp/latest/cli-reference/68020/get-started-with-the-lacework-forticnapp-cli)
- API Documentation: [About the Lacework FortiCNAPP API](https://docs.fortinet.com/document/lacework-forticnapp/latest/api-reference/863111/about-the-lacework-forticnapp-api)
- API Documentation: [Lacework API Documentation](https://yourlacework.lacework.net/api/v2/docs)
- Query Language Documentation: [LQL Overview](https://docs.fortinet.com/document/lacework-forticnapp/latest/lql-reference/598361/lql-overview)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

