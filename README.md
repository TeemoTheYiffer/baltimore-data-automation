# Baltimore Water Bill Scraper

A Python application for scraping water bill information from the Baltimore City water bill website and updating a Google Spreadsheet.

## Features

- Scrapes water bill details from the Baltimore City water bill website (https://pay.baltimorecity.gov/water/)
- Handles authentication via request verification tokens
- Processes address lookups to retrieve account numbers
- Retrieves detailed water bill information using account numbers
- Updates a Google Spreadsheet with the retrieved information
- Supports batch processing of multiple addresses
- Provides logging and error handling
- Includes options for testing single addresses or account numbers

## Prerequisites

- Python 3.8 or higher
- A Google service account with domain-wide delegation
- Access to the Google Sheets API
- The spreadsheet ID of the Google Sheet to update

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/baltimore-water-bills.git
   cd baltimore-water-bills
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Set up your secrets directory:
   ```
   mkdir -p secrets
   ```

4. Create a `google_credentials.py` file in the secrets directory with the following content:
   ```python
   # secrets/google_credentials.py
   SERVICE_ACCOUNT_FILE = "secrets/service_account.json"
   IMPERSONATED_USER = "your-user@domain.com"  # Replace with your actual email
   ```

5. Place your service account JSON file in the secrets directory:
   ```
   cp /path/to/your/service-account.json secrets/service_account.json
   ```

### Troubleshooting Credentials

If you encounter issues with credentials:

1. Make sure the `secrets` directory exists at the root of the project
2. Verify that `google_credentials.py` is in the `secrets` directory
3. Check that `SERVICE_ACCOUNT_FILE` points to the correct path of your service account JSON file
4. Ensure `IMPERSONATED_USER` is set to a valid email that has access to the spreadsheet
5. Confirm that the service account has domain-wide delegation permissions

You can run the script with the `--list-sheets` option to test if your credentials are working properly:
```
python main.py --list-sheets
```

If you still have issues, check the log file `baltimore_water.log` for more detailed error messages.

## Usage

### Preparing Your Spreadsheet

1. Create a Google Spreadsheet with the following structure:
   - Column A: Service addresses to look up
   - Columns B-I will be populated with the results

2. Share the spreadsheet with your service account email address

### Running the Script

#### Working with Multiple Sheets

To list all available sheets in the spreadsheet:

```
python main.py --list-sheets
```

To process all sheets automatically:

```
python main.py
```

To process a specific sheet:

```
python main.py --sheet "Sheet2"
```

To change the delay between requests:

```
python main.py --delay 5.0
```

### Testing

To test with a single address:

```
python main.py --address "1513 ABBOTSTON ST"
```

To test with a single account number:

```
python main.py --account "11000172386"
```

## File Structure

- `config.py`: Configuration settings
- `scraper.py`: Web scraping functionality
- `sheets.py`: Google Sheets integration
- `main.py`: Main execution script
- `secrets/`: Directory for credentials (not in version control)
  - `google_credentials.py`: Imported credentials
  - `service_account.json`: Service account credentials

## Requirements

Create a `requirements.txt` file with:

```
requests
beautifulsoup4
google-api-python-client
google-auth
google-auth-httplib2
google-auth-oauthlib
pydantic
```