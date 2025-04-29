import re
import time
import logging
from typing import Dict, Optional, Any, Union
import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup

from config import AppConfig
from utils.address_utils import parse_address, get_simplified_address

# Setup logging
logger = logging.getLogger("scraper")

class WaterBillScraper:
    """Scraper for Baltimore City water bill website."""
    
    def __init__(self, config: Optional[AppConfig] = None):
        """Initialize the scraper with settings."""
        self.config = config or AppConfig()
        self.session = requests.Session()
        self.verification_token = None
    
    def get_water_bill_details(self, service_address: str) -> Dict[str, Any]:
        """
        Get water bill details for a service address.
        
        Args:
            service_address: The address to look up
                
        Returns:
            Dictionary with bill details or error information
        """
        original_address = service_address
        
        try:
            # Parse and clean the address
            cleaned_address, _, _ = parse_address(service_address)
            
            # Try with the cleaned address first
            logger.info(f"Searching for cleaned address: {cleaned_address}")
            account_result = self.get_account_number_for_address(cleaned_address)
            
            # If the first attempt failed, try with a simplified address
            if not account_result.get("success"):
                simplified_address = get_simplified_address(service_address)
                if simplified_address != cleaned_address:
                    logger.info(f"Trying simplified address: {simplified_address}")
                    account_result = self.get_account_number_for_address(simplified_address)
            
            if not account_result.get("success"):
                return {
                    "success": False,
                    "message": f"No account found for address: {original_address}"
                }
            
            account_number = account_result.get("account_number")
            
            if not account_number:
                return {
                    "success": False,
                    "message": f"No account found for address: {original_address}"
                }
            
            # Step 2: Get bill details using the account number
            return self.get_bill_details_by_account_number(account_number)
                
        except Exception as e:
            logger.error(f"Error getting water bill details for {original_address}: {e}")
            return {
                "success": False,
                "message": f"Error: {str(e)}"
            }
    
    def get_account_number_for_address(self, service_address: str) -> Dict[str, Any]:
        """
        Get the account number for a service address.
        
        Args:
            service_address: The address to look up
            
        Returns:
            Dictionary with account number or error information
        """
        try:
            # Step 1: Get the main page to extract the verification token
            if not self.verification_token:
                self._fetch_verification_token()
            
            # Step 2: Search by service address
            search_endpoint = f"{self.config.BASE_URL}{self.config.ADDRESS_SEARCH_ENDPOINT}"
            search_data = {
                'ServiceAddress': service_address,
                '__RequestVerificationToken': self.verification_token
            }
            
            logger.info(f"Searching for address: {service_address}")
            
            response = self.session.post(
                search_endpoint,
                data=search_data,
                timeout=self.config.REQUEST_TIMEOUT,
                allow_redirects=False
            )
            
            # Check response
            if response.status_code != 200:
                logger.error(f"Error searching for address. Status code: {response.status_code}")
                return {
                    "success": False,
                    "message": f"Address search failed. Status code: {response.status_code}"
                }
            
            # Extract account number from response
            account_number = self._extract_account_number(response.text)
            
            if not account_number:
                return {
                    "success": False,
                    "message": f"Could not find account number for address: {service_address}"
                }
            
            return {
                "success": True,
                "account_number": account_number
            }
            
        except RequestException as e:
            logger.error(f"Request error searching for address: {e}")
            return {
                "success": False,
                "message": f"Network error: {str(e)}"
            }
        except Exception as e:
            logger.error(f"Error getting account number: {e}")
            return {
                "success": False,
                "message": f"Error: {str(e)}"
            }
    
    def get_bill_details_by_account_number(self, account_number: str) -> Dict[str, Any]:
        """
        Get bill details for an account number.
        
        Args:
            account_number: The account number
            
        Returns:
            Dictionary with bill details or error information
        """
        try:
            # Ensure we have a verification token
            if not self.verification_token:
                self._fetch_verification_token()
            
            # Submit account number search
            account_endpoint = f"{self.config.BASE_URL}{self.config.ACCOUNT_SEARCH_ENDPOINT}"
            account_data = {
                'AccountNumber': account_number,
                '__RequestVerificationToken': self.verification_token
            }
            
            logger.info(f"Searching for account: {account_number}")
            
            response = self.session.post(
                account_endpoint,
                data=account_data,
                timeout=self.config.REQUEST_TIMEOUT,
                allow_redirects=False
            )
            
            # Check if we got a redirect (302)
            if response.status_code == 302:
                # Get the redirect URL
                location = response.headers.get('Location')
                
                if not location:
                    return {
                        "success": False,
                        "message": "No redirect location found in response",
                        "account_number": account_number
                    }
                
                # Fix the URL if needed
                redirect_url = self._fix_redirect_url(location)
                
                # Follow the redirect
                bill_response = self.session.get(
                    redirect_url,
                    timeout=self.config.REQUEST_TIMEOUT
                )
                
                if bill_response.status_code != 200:
                    # Try alternative URL if 404
                    if bill_response.status_code == 404:
                        alt_url = f"{self.config.BASE_URL}bill"
                        bill_response = self.session.get(
                            alt_url,
                            timeout=self.config.REQUEST_TIMEOUT
                        )
                
                if bill_response.status_code == 200:
                    # Extract bill details from the page
                    return self._extract_bill_details(bill_response.text, account_number)
                
                return {
                    "success": False,
                    "message": f"Failed to load bill page. Status code: {bill_response.status_code}",
                    "account_number": account_number
                }
                
            elif response.status_code == 200:
                # If we didn't get a redirect but got a 200, try to extract bill details
                return self._extract_bill_details(response.text, account_number)
            
            else:
                return {
                    "success": False, 
                    "message": f"Failed to get bill details. Status code: {response.status_code}",
                    "account_number": account_number
                }
                
        except RequestException as e:
            logger.error(f"Request error getting bill details: {e}")
            return {
                "success": False,
                "message": f"Network error: {str(e)}",
                "account_number": account_number
            }
        except Exception as e:
            logger.error(f"Error getting bill details: {e}")
            return {
                "success": False,
                "message": f"Error: {str(e)}",
                "account_number": account_number
            }
    
    def _fetch_verification_token(self) -> None:
        """Fetch verification token from the main page."""
        try:
            response = self.session.get(
                self.config.BASE_URL,
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            response.raise_for_status()
            
            # Extract the verification token
            token_regex = r'<input name="__RequestVerificationToken" type="hidden" value="([^"]+)"'
            token_match = re.search(token_regex, response.text)
            
            if not token_match:
                logger.error("Could not extract verification token from page")
                raise ValueError("Could not extract verification token from page")
            
            self.verification_token = token_match.group(1)
            logger.info("Successfully extracted verification token")
            
        except Exception as e:
            logger.error(f"Error fetching verification token: {e}")
            raise
    
    def _extract_account_number(self, html_content: str) -> Optional[str]:
        """Extract account number from HTML response."""
        # Use BeautifulSoup for more robust extraction
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for account number in table cells
        # This might need adjustment based on actual HTML structure
        for td in soup.find_all('td'):
            # Check if the content looks like an account number (digits only)
            if td.text and td.text.strip().isdigit():
                return td.text.strip()
        
        # Fallback to regex if BeautifulSoup doesn't find it
        account_regex = r'<td>(\d+)</td>'
        account_match = re.search(account_regex, html_content)
        
        if account_match:
            return account_match.group(1)
        
        return None
    
    def _fix_redirect_url(self, location: str) -> str:
        """Fix redirect URL if needed."""
        if location.startswith('http'):
            return location
        elif location.startswith('/water/'):
            # Location starts with /water/, so use the domain only
            return f"https://pay.baltimorecity.gov{location}"
        else:
            # Location is a relative path without /water/
            return f"{self.config.BASE_URL}{location.lstrip('/')}"
    
    def _extract_bill_details(self, html_content: str, account_number: str) -> Dict[str, Any]:
        """Extract bill details from HTML content."""
        if not html_content or len(html_content) < 100:
            return {
                "success": False,
                "message": "Empty or too short bill content",
                "account_number": account_number
            }
        
        # Use BeautifulSoup for more robust extraction
        soup = BeautifulSoup(html_content, 'html.parser')
        
        extracted_data = {"account_number": account_number}
        
        # Define extraction patterns
        extraction_patterns = {
            "service_address": (r'<b>Service Address</b>\s*([^<]+)', "serviceAddress"),
            "bill_date": (r'<b>Current Bill Date</b>\s*(\d{2}/\d{2}/\d{4})', "billDate"),
            "current_bill_amount": (r'<b>Current Bill Amount</b>\s*\$\s*([\d.]+)', "currentBillAmount"),
            "previous_balance": (r'<b>Previous Balance</b>\s*\$\s*([\d.]+)', "previousBalance"),
            "current_balance": (r'<b>Current Balance</b>\s*\$\s*([\d.]+)', "currentBalance"),
            "penalty_date": (r'<b>Penalty Date</b>\s*(\d{2}/\d{2}/\d{4})', "penaltyDate"),
            "last_payment_date": (r'<b>Last Pay Date</b>\s*(\d{2}/\d{2}/\d{4})', "lastPaymentDate"),
            "last_payment_amount": (r'<b>Last Pay Amount</b>\s*\$\s*([-\d.]+)', "lastPaymentAmount"),
            "customer_name": (r'id="CustomerName"[^>]*value="([^"]+)"', "customerName")
        }
        
        # Extract data using regex patterns
        for key, (pattern, field_name) in extraction_patterns.items():
            match = re.search(pattern, html_content)
            if match:
                extracted_data[key] = match.group(1).strip()
        
        # Check if we extracted at least some data
        has_data = len(extracted_data) > 1
        
        return {
            "success": has_data,
            "data": extracted_data,
            "message": "Successfully retrieved bill details" if has_data else "Failed to extract bill data from response"
        }