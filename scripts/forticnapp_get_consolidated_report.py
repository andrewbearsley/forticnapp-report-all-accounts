#!/usr/bin/env python3
"""
Script to get consolidated compliance reports for all accounts in a Lacework instance.
Usage: python3 forticnapp_get_consolidated_report.py <api-key-path> <report-name> [options]
"""

import argparse
import datetime
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False


# ============================================================================
# Constants
# ============================================================================

class CloudType(Enum):
    """Supported cloud types."""
    AWS = 'aws'
    AZURE = 'azure'
    GCP = 'gcp'


class OutputFormat(Enum):
    """Supported output formats."""
    JSON = 'json'
    EXCEL = 'excel'


@dataclass
class Config:
    """Configuration constants."""
    # Rate limiting
    MAX_RETRIES: int = 5
    BACKOFF_INCREMENT: int = 30  # seconds (backoff delay when rate limited)
    
    # Request delays
    REQUEST_DELAY: float = 0.5  # seconds between requests (small delay to avoid hammering API)
    
    # Directories
    CACHE_DIR: str = 'cache'
    OUTPUT_DIR: str = 'output'
    
    # Default output files
    DEFAULT_EXCEL_OUTPUT: str = 'forticnapp-compliance-report.xlsx'
    DEFAULT_JSON_OUTPUT: str = 'forticnapp-compliance-report.json'
    
    # Excel formatting
    EXCEL_HEADER_COLOR: str = "366092"
    EXCEL_HEADER_TEXT_COLOR: str = "FFFFFF"
    EXCEL_LINK_COLOR: str = "0000FF"
    EXCEL_MAX_COLUMN_WIDTH: int = 50
    
    # Severity mapping
    SEVERITY_MAP: Dict[int, str] = None
    SEVERITY_ORDER: Dict[str, int] = None
    
    # Summary field mappings
    RECOMMENDATIONS_FIELDS: List[Tuple[str, str]] = None
    POLICY_FIELDS: List[Tuple[str, str]] = None
    RESOURCE_FIELDS: List[Tuple[str, str]] = None
    SUMMARY_FIELDS: List[Tuple[str, str]] = None
    
    # Excel column headers
    EXCEL_HEADERS: List[str] = None
    
    def __post_init__(self):
        if self.SEVERITY_MAP is None:
            self.SEVERITY_MAP = {
                1: 'Critical',
                2: 'High',
                3: 'Medium',
                4: 'Low',
                5: 'Info'
            }
        if self.SEVERITY_ORDER is None:
            self.SEVERITY_ORDER = {
                'Critical': 1,
                'High': 2,
                'Medium': 3,
                'Low': 4,
                'Info': 5
            }
        if self.RECOMMENDATIONS_FIELDS is None:
            # Policies overview section (total count)
            self.RECOMMENDATIONS_FIELDS = [
                ('NUM_RECOMMENDATIONS', 'Total Policies'),
            ]
        if self.POLICY_FIELDS is None:
            # Policy metrics (compliance recommendations/policies)
            self.POLICY_FIELDS = [
                ('NUM_COMPLIANT', 'Compliant'),
                ('NUM_NOT_COMPLIANT', 'Non-Compliant'),
                ('NUM_SEVERITY_1_NON_COMPLIANCE', 'Severity 1 (Critical)'),
                ('NUM_SEVERITY_2_NON_COMPLIANCE', 'Severity 2 (High)'),
                ('NUM_SEVERITY_3_NON_COMPLIANCE', 'Severity 3 (Medium)'),
                ('NUM_SEVERITY_4_NON_COMPLIANCE', 'Severity 4 (Low)'),
                ('NUM_SEVERITY_5_NON_COMPLIANCE', 'Severity 5 (Info)'),
                ('NUM_SUPPRESSED', 'Suppressed'),
            ]
        if self.RESOURCE_FIELDS is None:
            # Resource metrics (actual cloud resources)
            self.RESOURCE_FIELDS = [
                ('ASSESSED_RESOURCE_COUNT', 'Assessed'),
                ('VIOLATED_RESOURCE_COUNT', 'Violated'),
                ('SUPPRESSED_RESOURCE_COUNT', 'Suppressed'),
            ]
        if self.SUMMARY_FIELDS is None:
            # Combined for backward compatibility
            self.SUMMARY_FIELDS = self.RECOMMENDATIONS_FIELDS + self.POLICY_FIELDS + self.RESOURCE_FIELDS
        if self.EXCEL_HEADERS is None:
            self.EXCEL_HEADERS = [
                'Section', 'Service', 'Policy', 'Link', 'Severity', 'Account', 'Status',
                'Resource', 'Tags'
            ]


# Global config instance
CONFIG = Config()

# Rate limit patterns
RATE_LIMIT_PATTERNS = [
    r'HTTP.*429',
    r'status.*429',
    r'429.*Too Many',
    r'rate limit exceeded',
    r'rate\.limit\.exceeded',
    r'too many requests'
]


# ============================================================================
# Output Utilities
# ============================================================================

class Colors:
    """ANSI color codes for terminal output."""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    NC = '\033[0m'  # No Color


class Logger:
    """Centralized logging utility."""
    
    @staticmethod
    def info(msg: str) -> None:
        """Print info message."""
        print(f"{Colors.GREEN}[INFO]{Colors.NC} {msg}")

    @staticmethod
    def warning(msg: str) -> None:
        """Print warning message."""
        print(f"{Colors.YELLOW}[WARNING]{Colors.NC} {msg}", file=sys.stderr)

    @staticmethod
    def error(msg: str) -> None:
        """Print error message."""
        print(f"{Colors.RED}[ERROR]{Colors.NC} {msg}", file=sys.stderr)

    @staticmethod
    def verbose(msg: str, enabled: bool = False) -> None:
        """Print verbose message if enabled."""
        if enabled:
            print(f"{Colors.GREEN}[VERBOSE]{Colors.NC} {msg}", file=sys.stderr)


# ============================================================================
# Validation and Setup
# ============================================================================

def check_required_tools(output_format: OutputFormat) -> None:
    """Check if required tools are installed."""
    if not shutil.which("lacework"):
        Logger.error("lacework CLI is not installed. Please install it first.")
        sys.exit(1)

    if output_format == OutputFormat.EXCEL and not HAS_OPENPYXL:
        Logger.error("openpyxl is required for Excel output. Install it with: pip3 install openpyxl")
        sys.exit(1)


def load_api_key(api_key_path: str) -> Dict:
    """Load and validate API key from JSON file."""
    if not os.path.exists(api_key_path):
        Logger.error(f"API key file not found: {api_key_path}")
        sys.exit(1)

    with open(api_key_path, 'r') as f:
        api_key = json.load(f)
    
    required_fields = ['keyId', 'secret', 'account']
    missing = [field for field in required_fields if not api_key.get(field)]
    if missing:
        Logger.error(f"Invalid API key file. Missing required fields: {', '.join(missing)}")
        sys.exit(1)

    return api_key


def configure_lacework(api_key: Dict, verbose: bool) -> Dict[str, str]:
    """Configure Lacework CLI with API key and return environment variables."""
    Logger.verbose(f"Configuring Lacework CLI for account: {api_key['account']}", verbose)
    
    env = os.environ.copy()
    env['LW_ACCOUNT'] = api_key['account']
    env['LW_API_KEY'] = api_key['keyId']
    env['LW_API_SECRET'] = api_key['secret']
    
    if api_key.get('subAccount'):
        env['LW_SUBACCOUNT'] = api_key['subAccount']
        Logger.verbose(f"Using sub-account: {api_key['subAccount']}", verbose)
    
    # Test connection
    Logger.verbose("Testing Lacework CLI connection...", verbose)
    result = subprocess.run(
        ['lacework', 'configure', 'list'],
        capture_output=True,
        text=True,
        env=env
    )
    
    if result.returncode != 0:
        Logger.error("Failed to configure Lacework CLI. Please check your API key.")
        sys.exit(1)

    Logger.info("Successfully configured Lacework CLI")
    return env


# ============================================================================
# API Call Handling
# ============================================================================

def make_api_call(
    cmd: List[str],
    env: Dict[str, str],
    verbose: bool,
    retry_count: int = 0
) -> Tuple[str, bool]:
    """
    Make API call with rate limit handling.
    
    Returns:
        Tuple of (output, is_rate_limited)
    """
    while retry_count < CONFIG.MAX_RETRIES:
        Logger.verbose(f"Executing: {' '.join(cmd)}", verbose)
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=env
            )
            
            output = result.stdout + result.stderr
            is_rate_limited = _check_rate_limit(output, result.returncode)
            
            if is_rate_limited:
                _handle_rate_limit(retry_count, verbose, output)
                retry_count += 1
                continue
            
            if result.returncode != 0:
                # Check if output is valid JSON despite non-zero exit
                if _is_valid_json(output):
                    return output, False
                Logger.warning(f"Command failed with exit code {result.returncode}")
                Logger.verbose(f"Output: {output}", verbose)
                return "", False

            return output, False

        except Exception as e:
            Logger.warning(f"Error executing command: {e}")
            return "", False

    Logger.warning(f"Failed after {CONFIG.MAX_RETRIES} attempts")
    return "", False


def _check_rate_limit(output: str, exit_code: int) -> bool:
    """Check if output indicates rate limiting."""
    # Check explicit rate limit patterns
    for pattern in RATE_LIMIT_PATTERNS:
        if re.search(pattern, output, re.IGNORECASE):
            return True
    
    # Check if exit code is non-zero and output is not valid JSON
    if exit_code != 0 and not _is_valid_json(output):
        if re.search(r'(429|rate limit|too many requests)', output, re.IGNORECASE):
            return True
    
    return False


def _is_valid_json(text: str) -> bool:
    """Check if text is valid JSON."""
    try:
        json.loads(text)
        return True
    except (json.JSONDecodeError, ValueError):
        return False


def _handle_rate_limit(retry_count: int, verbose: bool, output: str) -> None:
    """Handle rate limit with incremental backoff."""
    if verbose:
        Logger.verbose("Received 429 response. Output was:", verbose)
        for line in output.split('\n'):
            if line.strip():
                Logger.verbose(f"  {line}", verbose)
    
    delay = CONFIG.BACKOFF_INCREMENT * (retry_count + 1)
    Logger.warning(
        f"Rate limited (429). Waiting {delay} seconds before "
        f"retry {retry_count + 1}/{CONFIG.MAX_RETRIES}..."
    )
    time.sleep(delay)


# ============================================================================
# Report Information
# ============================================================================

def get_report_info(report_name: str, env: Dict[str, str], verbose: bool) -> Optional[CloudType]:
    """Validate report name and determine cloud type."""
    Logger.verbose(f"Getting report definitions to validate: {report_name}", verbose)
    
    cmd = ['lacework', 'report-definitions', 'list', '--json']
    output, _ = make_api_call(cmd, env, verbose)
    
    try:
        definitions = json.loads(output)
        reports = _extract_reports_list(definitions)
        
        for report in reports:
            if report.get('reportName') == report_name:
                cloud_type = _determine_cloud_type(report.get('subReportType', ''))
                if cloud_type:
                    return cloud_type
        
        Logger.warning(f"Report '{report_name}' not found in report-definitions.")
        return None

    except json.JSONDecodeError:
        Logger.warning(f"Failed to parse report definitions response.")
        return None


def _extract_reports_list(definitions: Dict | List) -> List[Dict]:
    """Extract reports list from API response."""
    if isinstance(definitions, list):
        return definitions
    elif isinstance(definitions, dict) and 'data' in definitions:
        return definitions['data']
    return []


def _determine_cloud_type(sub_report_type: str) -> Optional[CloudType]:
    """Determine cloud type from sub-report type."""
    sub_type_lower = sub_report_type.lower()
    if sub_type_lower == 'aws':
        return CloudType.AWS
    elif sub_type_lower == 'azure':
        return CloudType.AZURE
    elif sub_type_lower in ['gcp', 'google']:
        return CloudType.GCP
    return None


# ============================================================================
# Account Fetching (with caching)
# ============================================================================

class AccountFetcher:
    """Base class for fetching cloud accounts with caching support."""
    
    def __init__(self, cloud_type: CloudType, env: Dict[str, str], verbose: bool, cache_dir: str):
        self.cloud_type = cloud_type
        self.env = env
        self.verbose = verbose
        self.cache_dir = cache_dir
        self.cache_file = os.path.join(cache_dir, f"{cloud_type.value}_accounts.txt")
    
    def get_accounts(self, use_cache: bool) -> List[str]:
        """Get accounts, using cache if requested."""
        if use_cache and os.path.exists(self.cache_file):
            Logger.verbose(f"Using cached {self.cloud_type.value} accounts", self.verbose)
            return self._read_cache()
        
        accounts = self._fetch_accounts()
        self._write_cache(accounts)
        return accounts
    
    def _read_cache(self) -> List[str]:
        """Read accounts from cache file."""
        with open(self.cache_file, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    
    def _write_cache(self, accounts: List[str]) -> None:
        """Write accounts to cache file."""
        os.makedirs(self.cache_dir, exist_ok=True)
        with open(self.cache_file, 'w') as f:
            for account in accounts:
                f.write(f"{account}\n")
    
    def _fetch_accounts(self) -> List[str]:
        """Fetch accounts from API. Must be implemented by subclasses."""
        raise NotImplementedError
    
    def _get_cloud_account_statuses(self) -> Dict[str, Dict]:
        """Get cloud account integration statuses for all cloud types."""
        try:
            cmd = ['lacework', 'cloud-account', 'list', '--json']
            output, _ = make_api_call(cmd, self.env, self.verbose)
            try:
                data = json.loads(output)
            except json.JSONDecodeError:
                return {}
            
            # Determine integration type and account identifier key based on cloud type
            integration_type_map = {
                CloudType.AWS: 'AwsCfg',
                CloudType.AZURE: 'AzureCfg',
                CloudType.GCP: 'GcpCfg',
            }
            
            account_id_key_map = {
                CloudType.AWS: 'awsAccountId',
                CloudType.AZURE: 'tenantId',  # Azure uses tenantId (or tenantGuid), we'll map tenant/subscription
                CloudType.GCP: 'projectId',  # GCP uses projectId, we'll map org/project
            }
            
            integration_type = integration_type_map.get(self.cloud_type)
            account_id_key = account_id_key_map.get(self.cloud_type)
            
            if not integration_type or not account_id_key:
                return {}
            
            # Filter for the appropriate integration type and map by account identifier
            statuses = {}
            for acc in data:
                if acc.get('type') == integration_type:
                    account_id = acc.get('data', {}).get(account_id_key)
                    # For Azure, also check tenantGuid if tenantId is not available
                    if not account_id and self.cloud_type == CloudType.AZURE:
                        account_id = acc.get('data', {}).get('tenantGuid')
                    if account_id:
                        # Store by the base identifier and let subclasses handle mapping
                        statuses[account_id] = {
                            'ok': acc.get('state', {}).get('ok', None),
                            'enabled': acc.get('enabled', 0),
                            'name': acc.get('name', ''),
                            'intgGuid': acc.get('intgGuid', ''),
                            'details': acc.get('state', {}).get('details', {}),
                            'data': acc.get('data', {})  # Store full data for mapping
                        }
            return statuses
        except Exception as e:
            Logger.verbose(f"Could not fetch cloud account statuses: {e}", self.verbose)
            return {}
    
    def _log_account_statuses(self, accounts: List[Dict], id_key: str, status_key: str, cloud_account_statuses: Dict[str, Dict] = None, skipped_accounts: List[Tuple[str, str]] = None) -> None:
        """Log account statuses for verbose output."""
        skipped_ids = {acc_id for acc_id, _ in (skipped_accounts or [])}
        
        Logger.verbose("Account statuses:", self.verbose)
        
        disabled_accounts = []
        enabled_accounts = []
        
        for acc in accounts:
            account_id = acc.get(id_key, 'Unknown')
            status = acc.get(status_key, 'Unknown')
            
            # Skip accounts that are already being skipped due to integration errors
            if account_id in skipped_ids:
                continue
            
            # Log account with all available details
            if self.verbose:
                details = [f"status={status}"]
                if cloud_account_statuses and account_id in cloud_account_statuses:
                    cloud_status = cloud_account_statuses[account_id]
                    intg_ok = "OK" if cloud_status.get('ok') else "ERROR"
                    details.append(f"integration={intg_ok}")
                    if cloud_status.get('name'):
                        details.append(f"name={cloud_status.get('name')}")
                if acc.get('state'):
                    details.append(f"state={acc.get('state')}")
                if acc.get('integration_state'):
                    details.append(f"integration_state={acc.get('integration_state')}")
                if acc.get('account_alias'):
                    details.append(f"alias={acc.get('account_alias')}")
                Logger.verbose(f"  {account_id}: {', '.join(details)}", self.verbose)
            
            # Categorize accounts
            if status == 'Disabled':
                disabled_accounts.append(account_id)
            elif status == 'Enabled':
                enabled_accounts.append(account_id)
        
        if disabled_accounts:
            Logger.verbose(f"Skipping {len(disabled_accounts)} disabled account(s)", self.verbose)
        
        Logger.verbose(f"Processing {len(enabled_accounts)} enabled account(s)", self.verbose)


class AWSAccountFetcher(AccountFetcher):
    """Fetcher for AWS accounts."""
    
    def _fetch_accounts(self) -> List[str]:
        """Fetch AWS accounts from AwsCfg integrations."""
        Logger.verbose("Fetching AWS accounts from cloud-account integrations...", self.verbose)
        
        # Get AwsCfg integrations with state.ok == True
        cloud_account_statuses = self._get_cloud_account_statuses()
        
        accounts = []
        skipped_accounts = []
        found_integrations = []
        
        # Get compliance account statuses to verify accounts are enabled
        try:
            cmd = ['lacework', 'compliance', 'aws', 'list-accounts', '--json']
            output, _ = make_api_call(cmd, self.env, self.verbose)
            data = json.loads(output)
            compliance_accounts = {acc.get('account_id'): acc.get('status', '') 
                                   for acc in data.get('aws_accounts', [])}
        except (json.JSONDecodeError, KeyError):
            compliance_accounts = {}
        
        # Use integrations as primary source
        for account_id, status_info in cloud_account_statuses.items():
            integration_name = status_info.get('name', 'Unknown')
            # Only include integrations with state.ok == True
            if status_info.get('ok') is not True:
                skipped_accounts.append((account_id, integration_name))
                continue
            
            found_integrations.append((account_id, integration_name))
            
            # Verify account is enabled in compliance system
            compliance_status = compliance_accounts.get(account_id, '')
            if compliance_status != 'Enabled':
                Logger.verbose(f"Skipping {account_id}: not enabled in compliance system (status: {compliance_status})", self.verbose)
                continue
            
            accounts.append(account_id)
        
        # Log found integrations
        if found_integrations:
            Logger.info(f"Found {len(found_integrations)} AwsCfg integration(s) with state.ok == True:")
            for account_id, name in found_integrations:
                Logger.info(f"  {name} (account: {account_id})")
        
        # Log skipped accounts
        if skipped_accounts:
            Logger.warning(f"Found {len(skipped_accounts)} account(s) with integration errors:")
            for account_id, name in skipped_accounts:
                Logger.warning(f"  {name} (account: {account_id}): AwsCfg integration error (state.ok != True)")
            Logger.warning("These accounts have integration issues and have been skipped.")
        
        if not accounts:
            Logger.warning("No AWS accounts found with AwsCfg integrations (state.ok == True)")
        
        return sorted(set(accounts))


class AzureAccountFetcher(AccountFetcher):
    """Fetcher for Azure subscriptions."""
    
    def _fetch_accounts(self) -> List[str]:
        """Fetch Azure subscriptions from AzureCfg integrations."""
        Logger.verbose("Fetching Azure subscriptions from cloud-account integrations...", self.verbose)
        
        # Get AzureCfg integrations with state.ok == True
        cloud_account_statuses = self._get_cloud_account_statuses()
        
        # Get tenant IDs from integrations (only those with state.ok == True)
        tenant_ids_from_integrations = []
        skipped_integrations = []
        found_integrations = []
        
        for tenant_id, status_info in cloud_account_statuses.items():
            integration_name = status_info.get('name', 'Unknown')
            if status_info.get('ok') is True:
                tenant_ids_from_integrations.append(tenant_id)
                found_integrations.append((tenant_id, integration_name))
            else:
                skipped_integrations.append((tenant_id, integration_name))
        
        # Log found integrations
        if found_integrations:
            Logger.info(f"Found {len(found_integrations)} AzureCfg integration(s) with state.ok == True:")
            for tenant_id, name in found_integrations:
                Logger.info(f"  {name} (tenant: {tenant_id})")
        
        # Log skipped integrations
        if skipped_integrations:
            Logger.warning(f"Found {len(skipped_integrations)} AzureCfg integration(s) with errors:")
            for tenant_id, name in skipped_integrations:
                Logger.warning(f"  {name} (tenant: {tenant_id}): AzureCfg integration error (state.ok != True)")
            Logger.warning("These integrations have issues and have been skipped.")
        
        # Try list-tenants command
        cmd = ['lacework', 'compliance', 'azure', 'list-tenants', '--json']
        output, _ = make_api_call(cmd, self.env, self.verbose)
        
        tenants = []
        try:
            data = json.loads(output)
            # Handle different response structures
            if 'azure_tenants' in data and data['azure_tenants']:
                tenants = data.get('azure_tenants', [])
            elif 'azure_subscriptions' in data and data['azure_subscriptions']:
                # If list-tenants returns subscriptions directly, use them
                # But we still need tenant_id, so use from integrations
                subscriptions = data.get('azure_subscriptions', [])
                if subscriptions and tenant_ids_from_integrations:
                    # Use first tenant ID from integrations
                    tenant_id = tenant_ids_from_integrations[0]
                    tenants = [{'tenant_id': tenant_id}]
        except (json.JSONDecodeError, KeyError):
            pass
        
        # Use tenant IDs from cloud-account integrations (primary source)
        if not tenants and tenant_ids_from_integrations:
            Logger.verbose(f"Using tenant IDs from cloud-account integrations: {tenant_ids_from_integrations}", self.verbose)
            tenants = [{'tenant_id': tid} for tid in tenant_ids_from_integrations]
        elif tenants:
            # Filter tenants to only those from our integrations
            tenants = [t for t in tenants if t.get('tenant_id') in tenant_ids_from_integrations]
        
        all_subscriptions = []
        subscriptions_json = []
        
        for tenant in tenants:
            tenant_id = tenant.get('tenant_id')
            if not tenant_id:
                continue
            
            cmd = ['lacework', 'compliance', 'azure', 'list-subscriptions', tenant_id, '--json']
            sub_output, _ = make_api_call(cmd, self.env, self.verbose)
            
            subscriptions = []
            if not sub_output or not sub_output.strip():
                # Empty response means no subscriptions configured
                Logger.verbose(f"No subscriptions found for tenant {tenant_id}", self.verbose)
            else:
                try:
                    sub_data = json.loads(sub_output)
                    if isinstance(sub_data, list):
                        # Response is a list — may contain objects with nested 'subscriptions' arrays
                        # e.g., [{"subscriptions": [{"id": "...", "alias": "..."}]}]
                        subscriptions = []
                        for item in sub_data:
                            if isinstance(item, dict) and 'subscriptions' in item:
                                subscriptions.extend(item['subscriptions'])
                            elif isinstance(item, dict):
                                subscriptions.append(item)
                        # If no nested subscriptions found, use the list as-is
                        if not subscriptions:
                            subscriptions = sub_data
                    elif isinstance(sub_data, dict):
                        subscriptions = sub_data.get('azure_subscriptions', [])
                    else:
                        subscriptions = []
                except json.JSONDecodeError as e:
                    Logger.verbose(f"Could not parse subscriptions JSON for tenant {tenant_id}: {e}", self.verbose)
                    subscriptions = []

                for sub in subscriptions:
                    # Handle both dict and string formats
                    if isinstance(sub, dict):
                        sub_id = sub.get('subscription_id') or sub.get('id')
                        status = sub.get('status', 'Enabled')
                    else:
                        # If subscription is just an ID string
                        sub_id = str(sub)
                        status = 'Enabled'
                    
                    if sub_id:
                        # Store subscription info with tenant_id for status checking
                        sub_info = {
                            'tenant_id': tenant_id,
                            'subscription_id': sub_id,
                            'status': status,
                            'account_key': f"{tenant_id}/{sub_id}"  # Format used in account list
                        }
                        subscriptions_json.append(sub_info)
                        all_subscriptions.append(sub_info)
            
        # Filter subscriptions: enabled only (integration status already checked)
        accounts = []
        
        for sub_info in all_subscriptions:
            status = sub_info['status']
            account_key = sub_info['account_key']
            
            # Skip disabled subscriptions
            if status != 'Enabled':
                continue
            
            # Integration status already checked (we only process tenants with state.ok == True)
            accounts.append(account_key)
        
        # Log account statuses
        self._log_account_statuses(subscriptions_json, 'account_key', 'status', cloud_account_statuses, None)
        
        if not accounts:
            if tenant_ids_from_integrations:
                Logger.warning(f"No Azure subscriptions found for tenant(s): {', '.join(tenant_ids_from_integrations)}")
                Logger.warning("Subscriptions may need to be discovered/enabled in Lacework for this tenant.")
            else:
                Logger.warning("No Azure subscriptions found. No AzureCfg integrations with state.ok == True found.")
        
        return sorted(set(accounts))


class GCPAccountFetcher(AccountFetcher):
    """Fetcher for GCP projects."""
    
    def _fetch_accounts(self) -> List[str]:
        """Fetch GCP projects from GcpCfg integrations."""
        Logger.verbose("Fetching GCP projects from cloud-account integrations...", self.verbose)
        
        # Get GcpCfg integrations with state.ok == True
        cloud_account_statuses = self._get_cloud_account_statuses()
        
        accounts = []
        skipped_accounts = []
        found_integrations = []
        
        # Get compliance project statuses to verify projects are enabled
        try:
            cmd = ['lacework', 'compliance', 'google', 'list', '--json']
            output, _ = make_api_call(cmd, self.env, self.verbose)
            data = json.loads(output)
            # Build a map of project_id -> (org_id, status)
            compliance_projects = {}
            for proj in data.get('gcp_projects', []):
                proj_id = proj.get('project_id', '')
                org_id = proj.get('organization_id', 'n/a')
                status = proj.get('status', '')
                if proj_id:
                    compliance_projects[proj_id] = (org_id, status)
        except (json.JSONDecodeError, KeyError):
            compliance_projects = {}
        
        # Use integrations as primary source
        for project_id, status_info in cloud_account_statuses.items():
            integration_name = status_info.get('name', 'Unknown')
            # Only include integrations with state.ok == True
            if status_info.get('ok') is not True:
                skipped_accounts.append((project_id, integration_name))
                continue
            
            found_integrations.append((project_id, integration_name))
            
            # Verify project is enabled in compliance system
            if project_id in compliance_projects:
                org_id, compliance_status = compliance_projects[project_id]
                if compliance_status != 'Enabled':
                    Logger.verbose(f"Skipping {project_id}: not enabled in compliance system (status: {compliance_status})", self.verbose)
                    continue
                
                # Build account key
                if org_id == 'n/a':
                    account_key = f"n/a/{project_id}"
                else:
                    account_key = f"{org_id}/{project_id}"
            else:
                # If not in compliance system, use project_id directly (format: n/a/project_id)
                account_key = f"n/a/{project_id}"
            
            accounts.append(account_key)
        
        # Log found integrations
        if found_integrations:
            Logger.info(f"Found {len(found_integrations)} GcpCfg integration(s) with state.ok == True:")
            for project_id, name in found_integrations:
                Logger.info(f"  {name} (project: {project_id})")
        
        # Log skipped accounts
        if skipped_accounts:
            Logger.warning(f"Found {len(skipped_accounts)} project(s) with integration errors:")
            for project_id, name in skipped_accounts:
                Logger.warning(f"  {name} (project: {project_id}): GcpCfg integration error (state.ok != True)")
            Logger.warning("These projects have integration issues and have been skipped.")
        
        if not accounts:
            Logger.warning("No GCP projects found with GcpCfg integrations (state.ok == True)")
        
        return sorted(set(accounts))


def get_accounts(
    cloud_type: CloudType,
    env: Dict[str, str],
    verbose: bool,
    use_cache: bool,
    cache_dir: str
) -> List[str]:
    """Get accounts for the specified cloud type."""
    fetcher_classes = {
        CloudType.AWS: AWSAccountFetcher,
        CloudType.AZURE: AzureAccountFetcher,
        CloudType.GCP: GCPAccountFetcher,
    }
    
    fetcher_class = fetcher_classes.get(cloud_type)
    if not fetcher_class:
        Logger.error(f"Unknown cloud type: {cloud_type}")
        sys.exit(1)

    fetcher = fetcher_class(cloud_type, env, verbose, cache_dir)
    return fetcher.get_accounts(use_cache)


# ============================================================================
# Report Fetching
# ============================================================================

def get_report_for_account(
    cloud_type: CloudType,
    report_name: str,
    account: str,
    output_file: str,
    env: Dict[str, str],
    verbose: bool
) -> bool:
    """Get report for a specific account via /api/v2/Reports."""
    Logger.verbose(f"Fetching report '{report_name}' for {cloud_type.value} account: {account}", verbose)

    cmd = _build_report_command(cloud_type, report_name, account)
    output, _ = make_api_call(cmd, env, verbose)

    if not output:
        Logger.warning(f"No report data returned for account: {account}")
        Logger.warning("This account may be disabled or have no compliance data available")
        return False

    # Parse and unwrap the API response
    try:
        api_response = json.loads(output)
    except (json.JSONDecodeError, ValueError):
        Logger.warning(f"Invalid JSON response for account: {account}")
        Logger.verbose(f"Response was: {output}", verbose)
        return False

    report_data = _unwrap_api_response(api_response, account, verbose)
    if report_data is None:
        return False

    # Log violation object structure for debugging
    if verbose:
        recs = report_data.get('recommendations', [])
        for r in recs:
            violations = r.get('VIOLATIONS', [])
            if violations:
                Logger.verbose(f"Violation object keys: {list(violations[0].keys())}", verbose)
                Logger.verbose(f"Sample violation: {json.dumps(violations[0], indent=2)}", verbose)
                break

    # Save the unwrapped report data
    with open(output_file, 'w') as f:
        json.dump(report_data, f, indent=2)

    Logger.verbose(f"Saved report for account '{account}' to: {output_file}", verbose)
    return True


def _build_report_command(cloud_type: CloudType, report_name: str, account: str) -> List[str]:
    """Build Lacework API command for getting a report via /api/v2/Reports."""
    encoded_name = urllib.parse.quote(report_name, safe='')
    params = f"format=json&reportName={encoded_name}"

    if cloud_type == CloudType.AWS:
        params += f"&primaryQueryId={account}"
    elif cloud_type == CloudType.AZURE:
        tenant_id, subscription_id = account.split('/', 1)
        params += f"&primaryQueryId={tenant_id}&secondaryQueryId={subscription_id}"
    elif cloud_type == CloudType.GCP:
        org_id, project_id = account.split('/', 1)
        params += f"&primaryQueryId={org_id}&secondaryQueryId={project_id}"
    else:
        Logger.error(f"Unknown cloud type: {cloud_type}")
        sys.exit(1)

    return ['lacework', 'api', 'get', f'api/v2/Reports?{params}', '--json', '--noninteractive']


def _unwrap_api_response(api_response, account: str, verbose: bool) -> Optional[Dict]:
    """
    Unwrap /api/v2/Reports response to match the format expected by downstream consumers.

    API returns: {"data": [{reportType, reportTitle, recommendations, summary, ...}]}
    Old CLI returned: {reportType, reportTitle, recommendations, summary, accountId, ...}

    Extracts data[0] and ensures accountId is populated.
    """
    if isinstance(api_response, dict) and 'data' in api_response:
        data_list = api_response['data']
        if not data_list:
            Logger.warning(f"Empty report data for account: {account}")
            return None
        report_data = data_list[0]
    elif isinstance(api_response, dict):
        report_data = api_response
    else:
        Logger.warning(f"Unexpected response format for account: {account}")
        Logger.verbose(f"Response type: {type(api_response)}", verbose)
        return None

    # Inject accountId if missing (old CLI included it, API may not)
    if not report_data.get('accountId'):
        report_data['accountId'] = account

    # Fallback reportTime if missing
    if not report_data.get('reportTime'):
        report_data['reportTime'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    return report_data


# ============================================================================
# Excel Generation
# ============================================================================

def create_excel_from_report(data: Dict, output_file: str, include_compliant: bool = False, tags_lookup: Dict = None) -> None:
    """Create Excel spreadsheet from report data."""
    if not HAS_OPENPYXL:
        Logger.error("openpyxl is required for Excel output. Install it with: pip3 install openpyxl")
        sys.exit(1)

    wb = Workbook()
    wb.remove(wb.active)  # Remove default sheet

    _create_summary_sheet(wb, data)
    _create_recommendations_sheet(wb, data, include_compliant, tags_lookup)

    wb.save(output_file)


def _create_summary_sheet(wb: Workbook, data: Dict) -> None:
    """Create Summary sheet in workbook."""
    ws = wb.create_sheet("Summary", 0)
    summary = data['summary'][0]
    
    # Calculate percentages
    total_policies = summary.get('NUM_RECOMMENDATIONS', 0)
    non_compliant_policies = summary.get('NUM_NOT_COMPLIANT', 0)
    policy_non_compliant_pct = (non_compliant_policies / total_policies * 100) if total_policies > 0 else 0
    
    assessed_resources = summary.get('ASSESSED_RESOURCE_COUNT', 0)
    violated_resources = summary.get('VIOLATED_RESOURCE_COUNT', 0)
    resource_violated_pct = (violated_resources / assessed_resources * 100) if assessed_resources > 0 else 0
    
    # Count "Could Not Assess" policies from recommendations
    recommendations = data.get('recommendations', [])
    could_not_assess_count = sum(1 for rec in recommendations if rec.get('STATUS', '').upper() == 'COULDNOTASSESS')
    
    # Analyze which accounts have "Could Not Assess" issues
    could_not_assess_by_account = {}
    for rec in recommendations:
        if rec.get('STATUS', '').upper() == 'COULDNOTASSESS':
            account_id = rec.get('ACCOUNT_ID', 'Unknown')
            could_not_assess_by_account[account_id] = could_not_assess_by_account.get(account_id, 0) + 1
    
    # Header section - vibrant design
    ws['A1'] = data['reportTitle']
    ws['A1'].font = Font(bold=True, size=16, color="1A1A1A")
    ws['A2'] = f"Report Type: {data['reportType']}"
    ws['A2'].font = Font(size=10, color="333333")
    ws['A3'] = f"Report Time: {data['reportTime']}"
    ws['A3'].font = Font(size=10, color="333333")
    
    # Spacing
    row = 5
    
    # Policies Section
    row += 1
    _add_section_header(ws, row, 'Policies')
    row += 1
    # 1. Non-Compliant % (right-aligned)
    _add_metric_row(ws, row, 'Non-Compliant %', f"{policy_non_compliant_pct:.1f}%", right_align=True)
    row += 1
    # 2. Non-Compliant
    _add_metric_row(ws, row, 'Non-Compliant', non_compliant_policies)
    row += 1
    # 3. Compliant
    compliant_policies = summary.get('NUM_COMPLIANT', 0)
    _add_metric_row(ws, row, 'Compliant', compliant_policies)
    row += 1
    # 4. Requires Manual Assessment (Could Not Assess)
    _add_metric_row(ws, row, 'Requires Manual Assessment', could_not_assess_count)
    row += 1
    # Show accounts with most "Could Not Assess" issues if any exist
    if could_not_assess_count > 0 and could_not_assess_by_account:
        top_accounts = sorted(could_not_assess_by_account.items(), key=lambda x: x[1], reverse=True)[:5]
        accounts_list = ', '.join([f"{acc}({count})" for acc, count in top_accounts])
        if len(could_not_assess_by_account) > 5:
            accounts_list += f" (+{len(could_not_assess_by_account) - 5} more)"
        _add_metric_row(ws, row, '  Top affected accounts', accounts_list)
        row += 1
    # 5. Total Policies
    total_policies = summary.get('NUM_RECOMMENDATIONS', 0)
    _add_metric_row(ws, row, 'Total Policies', total_policies)
    row += 1
    
    # Resources Section
    row += 1
    _add_section_header(ws, row, 'Resources')
    row += 1
    # 1. Violated % (right-aligned)
    _add_metric_row(ws, row, 'Violated %', f"{resource_violated_pct:.1f}%", right_align=True)
    row += 1
    # 2. Violated
    _add_metric_row(ws, row, 'Violated', violated_resources)
    row += 1
    # 3. Assessed
    assessed_resources = summary.get('ASSESSED_RESOURCE_COUNT', 0)
    _add_metric_row(ws, row, 'Assessed', assessed_resources)
    row += 1
    # 4. Suppressed
    suppressed_resources = summary.get('SUPPRESSED_RESOURCE_COUNT', 0)
    _add_metric_row(ws, row, 'Suppressed', suppressed_resources)
    row += 1
    
    # Column widths
    ws.column_dimensions['A'].width = 35
    ws.column_dimensions['B'].width = 25
    
    # Add notes at bottom
    note_row = row + 2
    note_cell = ws[f'A{note_row}']
    note_cell.value = "Note: Policy counts refer to compliance policies/checks. Resource counts refer to actual cloud resources assessed."
    note_cell.font = Font(italic=True, size=8, color="666666")
    note_cell.alignment = Alignment(wrap_text=True, vertical='top')
    ws.merge_cells(f'A{note_row}:B{note_row}')
    ws.row_dimensions[note_row].height = 25
    
    # Add note about "Could Not Assess" if applicable
    if could_not_assess_count > 0:
        note_row += 1
        cna_note_cell = ws[f'A{note_row}']
        cna_note_cell.value = "Could Not Assess: Compliance checks that could not be performed, often due to missing IAM permissions, services not enabled, or resources not available. Check account configuration and Lacework integration."
        cna_note_cell.font = Font(italic=True, size=8, color="D97706")
        cna_note_cell.alignment = Alignment(wrap_text=True, vertical='top')
        ws.merge_cells(f'A{note_row}:B{note_row}')
        ws.row_dimensions[note_row].height = 30


def _add_section_header(ws, row: int, title: str) -> None:
    """Add a section header with vibrant styling."""
    header_cell = ws[f'A{row}']
    header_cell.value = title
    header_cell.font = Font(bold=True, size=12, color="FFFFFF")
    header_cell.fill = PatternFill(start_color="4A5568", end_color="4A5568", fill_type="solid")
    ws.merge_cells(f'A{row}:B{row}')
    ws.row_dimensions[row].height = 24
    header_cell.alignment = Alignment(horizontal='left', vertical='center')


def _add_metric_row(ws, row: int, label: str, value, right_align: bool = False) -> None:
    """Add a metric row with vibrant styling."""
    ws[f'A{row}'] = label
    ws[f'A{row}'].font = Font(size=10, color="1A1A1A")
    ws[f'B{row}'] = value
    ws[f'B{row}'].font = Font(size=10, color="000000")
    if right_align:
        ws[f'B{row}'].alignment = Alignment(horizontal='right')
    ws.row_dimensions[row].height = 18


def _collect_violation_urns(recommendations: List[Dict]) -> List[str]:
    """Collect unique resource URNs from all violations across recommendations."""
    urns = set()
    for rec in recommendations:
        for v in rec.get('VIOLATIONS', []):
            resource = v.get('resource', '')
            if resource and resource.startswith('arn:'):
                urns.add(resource)
    return list(urns)


def _fetch_inventory_tags(
    urns: List[str],
    cloud_type: CloudType,
    env: Dict[str, str],
    verbose: bool
) -> Dict[str, Dict]:
    """
    Batch-fetch resource tags from the Inventory API.

    Calls POST /api/v2/Inventory/search with URN filters in batches of 100.
    Returns {urn: {tag_key: tag_value}} for resources with non-empty tags.
    On error, warns and returns partial results (never fails the report).
    """
    csp_map = {
        CloudType.AWS: 'AWS',
        CloudType.AZURE: 'Azure',
        CloudType.GCP: 'GCP',
    }
    csp = csp_map.get(cloud_type)
    if not csp:
        Logger.warning(f"Unknown cloud type for inventory lookup: {cloud_type}")
        return {}

    BATCH_SIZE = 100
    tags_lookup: Dict[str, Dict] = {}
    total_batches = (len(urns) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num in range(total_batches):
        start = batch_num * BATCH_SIZE
        batch = urns[start:start + BATCH_SIZE]
        Logger.verbose(
            f"Fetching inventory tags batch {batch_num + 1}/{total_batches} "
            f"({len(batch)} URNs)",
            verbose
        )

        payload = json.dumps({
            "csp": csp,
            "filters": [
                {"field": "urn", "expression": "in", "values": batch}
            ],
            "returns": ["urn", "resourceTags"]
        })

        cmd = [
            'lacework', 'api', 'post', 'api/v2/Inventory/search',
            '-d', payload, '--json', '--noninteractive'
        ]

        try:
            output, _ = make_api_call(cmd, env, verbose)
            if not output:
                Logger.warning(f"Empty response for inventory batch {batch_num + 1}")
                continue

            response = json.loads(output)
            data = response.get('data', []) if isinstance(response, dict) else []

            for item in data:
                urn = item.get('urn', '')
                resource_tags = item.get('resourceTags', {})
                # First non-empty tags per URN wins (multiple rows possible per URN)
                if urn and resource_tags and urn not in tags_lookup:
                    tags_lookup[urn] = resource_tags

        except (json.JSONDecodeError, ValueError) as e:
            Logger.warning(f"Failed to parse inventory response for batch {batch_num + 1}: {e}")
            continue
        except Exception as e:
            Logger.warning(f"Error fetching inventory batch {batch_num + 1}: {e}")
            continue

        # Small delay between batches
        if batch_num < total_batches - 1:
            time.sleep(CONFIG.REQUEST_DELAY)

    Logger.verbose(f"Inventory tag lookup complete: {len(tags_lookup)} resources with tags", verbose)
    return tags_lookup


def _expand_recommendations_to_rows(recommendations: List[Dict], include_compliant: bool = False, tags_lookup: Dict = None) -> List[Dict]:
    """
    Expand recommendations so each violation gets its own row.

    For non-compliant recommendations with violations: one row per violation,
    carrying the policy metadata plus the individual resource and tags.
    For compliant recommendations (when include_compliant=True): one row with
    empty Resource/Tags.
    For non-compliant with no violations: one row with empty Resource/Tags.
    """
    rows = []
    for rec in recommendations:
        status = rec.get('STATUS', '')
        violations = rec.get('VIOLATIONS', [])
        is_non_compliant = status.lower() == 'noncompliant'

        if not is_non_compliant and not include_compliant:
            continue

        # Base row data shared across all expanded rows for this recommendation
        base = {
            'CATEGORY': rec.get('CATEGORY', ''),
            'SERVICE': rec.get('SERVICE', ''),
            'TITLE': rec.get('TITLE', ''),
            'REC_ID': rec.get('REC_ID', ''),
            'SEVERITY': rec.get('SEVERITY', ''),
            'ACCOUNT_ID': rec.get('ACCOUNT_ID', ''),
            'STATUS': status,
        }

        if violations:
            for v in violations:
                row = dict(base)
                resource = v.get('resource', '')
                row['RESOURCE'] = resource
                # Prefer tags from inventory lookup, fall back to inline tags
                tags = (tags_lookup.get(resource, {}) if tags_lookup else {}) or v.get('resourceTags') or v.get('tags') or {}
                row['TAGS'] = json.dumps(tags) if tags else ''
                rows.append(row)
        else:
            row = dict(base)
            row['RESOURCE'] = ''
            row['TAGS'] = ''
            rows.append(row)

    return rows


def _create_recommendations_sheet(wb: Workbook, data: Dict, include_compliant: bool = False, tags_lookup: Dict = None) -> None:
    """Create Recommendations sheet in workbook."""
    ws = wb.create_sheet("Recommendations", 1)
    recommendations = data.get('recommendations', [])

    if not recommendations:
        return

    # Expand violations to individual rows
    expanded_rows = _expand_recommendations_to_rows(recommendations, include_compliant, tags_lookup)

    if not expanded_rows:
        return

    # Sort expanded rows
    expanded_rows = sorted(expanded_rows, key=_recommendation_sort_key)

    # Write headers
    _write_excel_headers(ws, CONFIG.EXCEL_HEADERS)

    # Write data rows
    for row_num, rec in enumerate(expanded_rows, 2):
        _write_recommendation_row(ws, row_num, rec)

    # Enable auto-filter
    ws.auto_filter.ref = ws.dimensions

    # Auto-adjust column widths
    _adjust_column_widths(ws, len(CONFIG.EXCEL_HEADERS))


def _recommendation_sort_key(rec: Dict) -> Tuple:
    """Generate sort key for recommendations."""
    category = rec.get('CATEGORY', '')
    service = rec.get('SERVICE', '')
    severity_num = rec.get('SEVERITY', 99)
    severity_label = CONFIG.SEVERITY_MAP.get(severity_num, 'Unknown')
    rec_id = rec.get('REC_ID', '')
    account_id = rec.get('ACCOUNT_ID', '')
    resource = rec.get('RESOURCE', '')

    # Service blank last
    service_sort = (1 if service else 2, service)

    return (
        category,
        service_sort,
        CONFIG.SEVERITY_ORDER.get(severity_label, 99),
        rec_id,
        account_id,
        resource
    )


def _write_excel_headers(ws, headers: List[str]) -> None:
    """Write headers to Excel worksheet."""
    header_fill = PatternFill(
        start_color=CONFIG.EXCEL_HEADER_COLOR,
        end_color=CONFIG.EXCEL_HEADER_COLOR,
        fill_type="solid"
    )
    header_font = Font(bold=True, color=CONFIG.EXCEL_HEADER_TEXT_COLOR)
    
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_num)
        cell.value = header
        cell.font = header_font
        cell.fill = header_fill


def _write_recommendation_row(ws, row_num: int, rec: Dict) -> None:
    """Write a single recommendation row to Excel worksheet."""
    # Column mapping: Section, Service, Policy, Link, Severity, Account, Status,
    # Resource, Tags
    ws.cell(row=row_num, column=1, value=rec.get('CATEGORY', ''))
    ws.cell(row=row_num, column=2, value=rec.get('SERVICE', ''))
    ws.cell(row=row_num, column=3, value=rec.get('TITLE', ''))

    # REC_ID as hyperlink
    rec_id = rec.get('REC_ID', '')
    if rec_id:
        rec_id_upper = rec_id.upper().replace('-', '_')
        url = f"https://docs.fortinet.com/document/lacework-forticnapp/latest/lacework-forticnapp-policies?cshid={rec_id_upper}"
        cell = ws.cell(row=row_num, column=4)
        cell.value = rec_id
        cell.hyperlink = url
        cell.font = Font(color=CONFIG.EXCEL_LINK_COLOR, underline="single")
    else:
        ws.cell(row=row_num, column=4, value='')

    # Severity as label
    severity = rec.get('SEVERITY', '')
    ws.cell(row=row_num, column=5, value=CONFIG.SEVERITY_MAP.get(severity, f'Unknown ({severity})'))

    ws.cell(row=row_num, column=6, value=rec.get('ACCOUNT_ID', ''))
    # Format status for readability
    status = rec.get('STATUS', '')
    ws.cell(row=row_num, column=7, value=_format_status(status))

    # Individual resource and tags (from expanded row)
    ws.cell(row=row_num, column=8, value=rec.get('RESOURCE', ''))
    ws.cell(row=row_num, column=9, value=rec.get('TAGS', ''))


def _format_status(status: str) -> str:
    """Format status value for better readability."""
    if not status:
        return ''
    # Convert camelCase/PascalCase to readable format
    # e.g., "CouldNotAssess" -> "Could Not Assess"
    # Insert space before capital letters (but not at the start)
    formatted = re.sub(r'(?<!^)(?=[A-Z])', ' ', status)
    return formatted


def _adjust_column_widths(ws, num_columns: int) -> None:
    """Auto-adjust column widths in worksheet."""
    for col_num in range(1, num_columns + 1):
        max_length = 0
        column = get_column_letter(col_num)
        for cell in ws[column]:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except (TypeError, AttributeError):
                pass
        adjusted_width = min(max_length + 2, CONFIG.EXCEL_MAX_COLUMN_WIDTH)
        ws.column_dimensions[column].width = adjusted_width


# ============================================================================
# Report Concatenation
# ============================================================================

def concatenate_reports(
    report_dir: str,
    output_format: OutputFormat,
    output_file: Optional[str],
    verbose: bool,
    include_compliant: bool = False,
    env: Dict[str, str] = None,
    cloud_type: CloudType = None,
    skip_tags: bool = False
) -> str:
    """Concatenate individual report files into a single consolidated report."""
    Logger.verbose("Collecting recommendations from all accounts...", verbose)

    report_files = _find_report_files(report_dir)
    if not report_files:
        raise FileNotFoundError(f"No JSON report files found in: {report_dir}")

    Logger.verbose(f"Found {len(report_files)} report file(s)", verbose)

    # Extract metadata from first file
    metadata = _extract_metadata(report_files[0])
    Logger.verbose(f"Report: {metadata['title']}", verbose)
    Logger.verbose(f"Type: {metadata['type']}", verbose)
    Logger.verbose(f"Time: {metadata['time']}", verbose)

    # Collect all recommendations and calculate summary
    all_recommendations, all_accounts = _collect_recommendations(report_files, verbose)
    total_recommendations = len(all_recommendations)
    Logger.verbose(f"Total recommendations collected: {total_recommendations}", verbose)

    # Fetch resource tags from inventory API
    tags_lookup = {}
    if env and cloud_type and not skip_tags and output_format != OutputFormat.JSON:
        urns = _collect_violation_urns(all_recommendations)
        if urns:
            Logger.info(f"Fetching tags for {len(urns)} unique resources from inventory...")
            tags_lookup = _fetch_inventory_tags(urns, cloud_type, env, verbose)
            Logger.info(f"Retrieved tags for {len(tags_lookup)} resources")
        else:
            Logger.verbose("No ARN resources found in violations, skipping tag fetch", verbose)

    Logger.verbose("Calculating overall summary...", verbose)
    summary = _calculate_aggregate_summary(report_files)

    # Create consolidated data
    consolidated_data = {
        'reportTitle': metadata['title'],
        'reportType': metadata['type'],
        'reportTime': metadata['time'],
        'recommendations': all_recommendations,
        'summary': [summary]
    }

    # Determine output file (default to output directory)
    output_file = _determine_output_file(output_file, output_format, report_dir)
    _ensure_output_directory(output_file, verbose)

    Logger.verbose(f"Output file: {output_file}", verbose)

    # Generate output
    if output_format == OutputFormat.JSON:
        _write_json_output(consolidated_data, output_file, verbose)
    else:
        _write_excel_output(consolidated_data, output_file, verbose, include_compliant, tags_lookup)

    # Print summary
    _print_concatenation_summary(all_accounts, total_recommendations, output_file)

    return report_dir


def _find_report_files(report_dir: str) -> List[Path]:
    """Find all JSON report files in directory."""
    report_files = []
    for file in sorted(Path(report_dir).glob('*.json')):
        if file.name != 'all_accounts.json':  # Exclude previously generated files
            report_files.append(file)
    return report_files


def _extract_metadata(report_file: Path) -> Dict[str, str]:
    """Extract metadata from first report file."""
    with open(report_file, 'r') as f:
        data = json.load(f)
    return {
        'title': data.get('reportTitle', ''),
        'type': data.get('reportType', ''),
        'time': data.get('reportTime', '')
    }


def _collect_recommendations(report_files: List[Path], verbose: bool) -> Tuple[List[Dict], List[str]]:
    """Collect all recommendations from report files."""
    all_recommendations = []
    all_accounts = []
    
    for report_file in report_files:
        with open(report_file, 'r') as f:
            data = json.load(f)
        
        account_id = data.get('accountId', '')
        account_alias = data.get('accountAlias', '')
        all_accounts.append(account_id)
        
        Logger.verbose(f"Processing account: {account_id} ({account_alias})", verbose)
        
        recommendations = data.get('recommendations', [])
        all_recommendations.extend(recommendations)
    
    return all_recommendations, all_accounts


def _calculate_aggregate_summary(report_files: List[Path]) -> Dict:
    """Calculate aggregate summary statistics from all reports."""
    summary_fields = [
        'ASSESSED_RESOURCE_COUNT', 'NUM_COMPLIANT', 'NUM_NOT_COMPLIANT',
        'NUM_RECOMMENDATIONS', 'NUM_SEVERITY_1_NON_COMPLIANCE',
        'NUM_SEVERITY_2_NON_COMPLIANCE', 'NUM_SEVERITY_3_NON_COMPLIANCE',
        'NUM_SEVERITY_4_NON_COMPLIANCE', 'NUM_SEVERITY_5_NON_COMPLIANCE',
        'NUM_SUPPRESSED', 'SUPPRESSED_RESOURCE_COUNT', 'VIOLATED_RESOURCE_COUNT'
    ]
    
    totals = {field: 0 for field in summary_fields}
    
    for report_file in report_files:
        with open(report_file, 'r') as f:
            data = json.load(f)
        
        summary = data.get('summary', [{}])[0]
        for field in summary_fields:
            totals[field] += summary.get(field, 0)
    
    return totals


def _determine_output_file(output_file: Optional[str], output_format: OutputFormat, report_dir: str) -> str:
    """Determine output file path."""
    if output_file:
        return output_file
    
    # Default to output directory
    output_dir = CONFIG.OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)
    
    if output_format == OutputFormat.JSON:
        return os.path.join(output_dir, CONFIG.DEFAULT_JSON_OUTPUT)
    return os.path.join(output_dir, CONFIG.DEFAULT_EXCEL_OUTPUT)


def _ensure_output_directory(output_file: str, verbose: bool) -> None:
    """Ensure output directory exists."""
    output_dir = os.path.dirname(output_file)
    if output_dir and output_dir not in ['.', './']:
        os.makedirs(output_dir, exist_ok=True)
        Logger.verbose(f"Created output directory: {output_dir}", verbose)


def _write_json_output(data: Dict, output_file: str, verbose: bool) -> None:
    """Write consolidated data as JSON."""
    Logger.verbose("Creating single concatenated report file (JSON)...", verbose)
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    Logger.info(f"Saved concatenated report to: {output_file}")


def _write_excel_output(data: Dict, output_file: str, verbose: bool, include_compliant: bool = False, tags_lookup: Dict = None) -> None:
    """Write consolidated data as Excel."""
    Logger.verbose("Creating Excel spreadsheet...", verbose)
    create_excel_from_report(data, output_file, include_compliant, tags_lookup)
    Logger.info(f"Saved Excel spreadsheet to: {output_file}")


def _print_concatenation_summary(accounts: List[str], recommendations: int, output_file: str) -> None:
    """Print concatenation summary."""
    print()
    Logger.info("Summary:")
    Logger.info(f"  Processed {len(accounts)} account(s)")
    Logger.info(f"  Total recommendations: {recommendations}")
    print()
    Logger.info(f"Output file: {output_file}")


def investigate_account(data: Dict, account_id: str) -> None:
    """Investigate 'Could Not Assess' issues for a specific account."""
    recommendations = data.get('recommendations', [])
    
    # Filter for the specific account
    account_recs = [rec for rec in recommendations if rec.get('ACCOUNT_ID', '') == account_id]
    
    if not account_recs:
        Logger.warning(f"No recommendations found for account: {account_id}")
        return
    
    # Filter for "Could Not Assess" entries
    could_not_assess = [rec for rec in account_recs if rec.get('STATUS', '').upper() == 'COULDNOTASSESS']
    
    print()
    Logger.info(f"Investigation Report for Account: {account_id}")
    Logger.info("=" * 70)
    print()
    
    Logger.info(f"Total recommendations for this account: {len(account_recs)}")
    Logger.info(f"'Could Not Assess' entries: {len(could_not_assess)}")
    print()
    
    if not could_not_assess:
        Logger.info("No 'Could Not Assess' issues found for this account.")
        return
    
    # Group by service and policy
    by_service = {}
    by_policy = {}
    
    for rec in could_not_assess:
        service = rec.get('SERVICE', 'N/A')
        policy_id = rec.get('REC_ID', 'N/A')
        policy_title = rec.get('TITLE', 'N/A')
        
        if service not in by_service:
            by_service[service] = []
        by_service[service].append(rec)
        
        if policy_id not in by_policy:
            by_policy[policy_id] = {
                'title': policy_title,
                'service': service,
                'count': 0
            }
        by_policy[policy_id]['count'] += 1
    
    # Show summary by service
    Logger.info("Breakdown by Service:")
    for service, recs in sorted(by_service.items()):
        Logger.info(f"  {service}: {len(recs)} policy(ies)")
    print()
    
    # Show detailed policy list
    Logger.info("Affected Policies:")
    for policy_id, info in sorted(by_policy.items(), key=lambda x: x[1]['count'], reverse=True):
        Logger.info(f"  [{info['count']} occurrence(s)] {policy_id}")
        Logger.info(f"    Service: {info['service']}")
        Logger.info(f"    Policy: {info['title']}")
        Logger.info(f"    Resource Count: {sum(r.get('RESOURCE_COUNT', 0) for r in could_not_assess if r.get('REC_ID') == policy_id)}")
        Logger.info(f"    Assessed Count: {sum(r.get('ASSESSED_RESOURCE_COUNT', 0) for r in could_not_assess if r.get('REC_ID') == policy_id)}")
        print()
    
    # Common causes analysis
    Logger.info("Possible Causes:")
    Logger.info("  1. Missing IAM Permissions: Lacework integration role may lack permissions")
    Logger.info("     to access certain AWS services or resources in this account.")
    Logger.info("  2. Service Not Enabled: Required AWS services may not be enabled")
    Logger.info("     (e.g., IAM Access Analyzer, Config, etc.)")
    Logger.info("  3. Resource Not Available: The resource type may not exist in this account.")
    Logger.info("  4. Integration Issues: Lacework may not be properly integrated with this account.")
    print()
    Logger.info("Recommendations:")
    Logger.info("  - Check Lacework integration status for this account")
    Logger.info("  - Verify IAM permissions for Lacework integration role")
    Logger.info("  - Ensure required AWS services are enabled")
    Logger.info("  - Review account configuration in Lacework console")
    print()


def _load_consolidated_data(report_output_dir: str, verbose: bool) -> Optional[Dict]:
    """Load consolidated data from individual report files for investigation."""
    report_files = []
    for file in sorted(Path(report_output_dir).glob('*.json')):
        if file.name != 'all_accounts.json':
            report_files.append(file)
    if not report_files:
        return None
    all_recommendations, _ = _collect_recommendations(report_files, verbose)
    metadata = _extract_metadata(report_files[0])
    summary = _calculate_aggregate_summary(report_files)
    return {
        'reportTitle': metadata['title'],
        'reportType': metadata['type'],
        'reportTime': metadata['time'],
        'recommendations': all_recommendations,
        'summary': [summary]
    }


# ============================================================================
# Main Function
# ============================================================================

def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Get compliance reports for all accounts in a Lacework instance',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s api-key/api-key.json "CIS Amazon Web Services Foundations Benchmark v1.4.0"
  %(prog)s api-key/api-key.json "My Custom AWS Framework" --cloud-type aws
  %(prog)s api-key/api-key.json "CIS Microsoft Azure Foundations Benchmark v1.5.0" -v
  %(prog)s api-key/api-key.json "CIS Amazon Web Services Foundations Benchmark v1.4.0" -f json
  %(prog)s api-key/api-key.json "CIS Amazon Web Services Foundations Benchmark v1.4.0" -o my-report.xlsx
  %(prog)s api-key/api-key.json "CIS Amazon Web Services Foundations Benchmark v1.4.0" --no-concatenate

To find available compliance reports:
  lacework report-definitions list                    (may not show custom frameworks)

For custom frameworks, specify --cloud-type explicitly:
  %(prog)s api-key/api-key.json "My Custom Report" --cloud-type aws
        """
    )
    
    parser.add_argument('api_key_path', help='Path to the Lacework API key JSON file')
    parser.add_argument('report_name', help='Name of the compliance report to fetch')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('--use-cache', action='store_true', help='Use cached account list data')
    parser.add_argument('--no-concatenate', action='store_true', help='Skip concatenation (only save individual JSON files)')
    parser.add_argument('--keep-intermediate', action='store_true', help='Keep intermediate JSON files in output/ directory after concatenation')
    parser.add_argument('-f', '--format', choices=['json', 'excel'], default='excel', help='Output format for concatenation (default: excel)')
    parser.add_argument('-o', '--output', help='Output file path for concatenated report')
    parser.add_argument('--cloud-type', choices=['aws', 'azure', 'gcp'],
                        help='Cloud type override. Required for custom frameworks not listed in report-definitions.')
    parser.add_argument('--include-compliant', action='store_true',
                        help='Include compliant policies (no violations) in the Excel output')
    parser.add_argument('--test', action='store_true',
                        help='Test mode: limit to first 3 accounts')
    parser.add_argument('--skip-tags', action='store_true',
                        help='Skip fetching resource tags from inventory API')
    parser.add_argument('--investigate-account', help='Investigate "Could Not Assess" issues for a specific account ID')
    
    args = parser.parse_args()
    
    # Convert format string to enum
    output_format = OutputFormat.JSON if args.format == 'json' else OutputFormat.EXCEL
    check_required_tools(output_format)
    
    Logger.info("Starting compliance report collection...")
    Logger.info(f"Report: {args.report_name}")
    Logger.info(f"API Key: {args.api_key_path}")
    
    # Load and configure API key
    api_key = load_api_key(args.api_key_path)
    env = configure_lacework(api_key, args.verbose)
    
    # Get report type (cloud type)
    if args.cloud_type:
        cloud_type = CloudType(args.cloud_type)
        Logger.info(f"Using specified cloud type: {cloud_type.value}")
    else:
        Logger.info("Validating report and determining cloud type...")
        cloud_type = get_report_info(args.report_name, env, args.verbose)
        if not cloud_type:
            Logger.error(
                f"Failed to determine cloud type for report '{args.report_name}'. "
                "This may be a custom framework not listed in report-definitions. "
                "Try specifying --cloud-type aws|azure|gcp explicitly."
            )
            sys.exit(1)
        Logger.info(f"Report type: {cloud_type.value}")
    
    # Get accounts list
    Logger.info(f"Fetching list of {cloud_type.value} accounts...")
    accounts = get_accounts(cloud_type, env, args.verbose, args.use_cache, CONFIG.CACHE_DIR)
    
    if not accounts:
        Logger.warning(f"No accounts found for cloud type: {cloud_type.value}")
        sys.exit(0)
    
    if args.test:
        accounts = accounts[:3]
        Logger.info(f"Test mode: limiting to first {len(accounts)} account(s)")

    account_count = len(accounts)
    Logger.info(f"Found {account_count} account(s)")
    
    # Create output directory
    report_output_dir = os.path.join(CONFIG.OUTPUT_DIR, f"{args.report_name}_{cloud_type.value}")
    os.makedirs(report_output_dir, exist_ok=True)
    Logger.info(f"Output directory: {report_output_dir}")
    
    # Fetch report for each account
    success_count = 0
    failure_count = 0
    
    for account_num, account in enumerate(accounts, 1):
        Logger.info(f"[{account_num}/{account_count}] Processing account: {account}")

        # Sanitize account name for filename
        safe_account_name = account.replace('/', '_').replace(':', '_')
        output_file = os.path.join(report_output_dir, f"{safe_account_name}.json")

        if get_report_for_account(cloud_type, args.report_name, account, output_file, env, args.verbose):
            success_count += 1
        else:
            failure_count += 1
            Logger.warning(f"Failed to get report for account: {account}")
            # Fail fast: if the first account returns no data, the report name is likely wrong
            if account_num == 1:
                Logger.error(
                    f"First account returned no data. Report name '{args.report_name}' "
                    "may not exist. Please verify the report name and try again."
                )
                sys.exit(1)
        
        # Small delay between requests
        time.sleep(CONFIG.REQUEST_DELAY)
    
    # Summary
    print()
    Logger.info("Summary:")
    Logger.info(f"  Total accounts: {account_count}")
    Logger.info(f"  Successful: {success_count}")
    Logger.info(f"  Failed: {failure_count}")
    if args.no_concatenate:
        Logger.info(f"  Individual reports saved to: {report_output_dir}")
    
    if failure_count > 0:
        sys.exit(1)
    
    # Concatenate reports if requested (default is True)
    consolidated_data = None
    if not args.no_concatenate:
        print()
        Logger.info("Concatenating reports...")
        try:
            concatenate_reports(report_output_dir, output_format, args.output, args.verbose,
                                args.include_compliant, env=env, cloud_type=cloud_type,
                                skip_tags=args.skip_tags)
            Logger.verbose("Successfully created concatenated report", args.verbose)
            
            # Load consolidated data for investigation if needed (before cleanup)
            if args.investigate_account:
                consolidated_data = _load_consolidated_data(report_output_dir, args.verbose)
            
            # Clean up intermediate files unless --keep-intermediate is specified
            if not args.keep_intermediate:
                Logger.verbose("Cleaning up intermediate files...", args.verbose)
                try:
                    shutil.rmtree(report_output_dir)
                    Logger.verbose(f"Removed directory: {report_output_dir}", args.verbose)
                    Logger.verbose("Cleaned up intermediate JSON files", args.verbose)
                except Exception as e:
                    Logger.warning(f"Failed to clean up intermediate files: {e}")
        except Exception as e:
            Logger.error(f"Failed to concatenate reports: {e}")
            Logger.warning("Intermediate files preserved for debugging")
            sys.exit(1)
    elif args.investigate_account:
        consolidated_data = _load_consolidated_data(report_output_dir, args.verbose)
    
    # Run investigation if requested
    if args.investigate_account:
        if not consolidated_data:
            Logger.error("Cannot investigate account: No consolidated data available. Run without --no-concatenate or ensure intermediate files exist.")
            sys.exit(1)
            sys.exit(1)
        investigate_account(consolidated_data, args.investigate_account)


if __name__ == '__main__':
    main()
