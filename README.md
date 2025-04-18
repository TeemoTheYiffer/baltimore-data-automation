# Baltimore Data Automation

A Python application for scraping water bill information from the Baltimore City water bill website and retrieving Maryland property data, then updating a Google Spreadsheet with the information.

## Features

### Water Bill Scraping
- Scrapes water bill details from the Baltimore City water bill website (https://pay.baltimorecity.gov/water/)
- Handles authentication via request verification tokens
- Processes address lookups to retrieve account numbers
- Retrieves detailed water bill information using account numbers
- Updates a Google Spreadsheet with the retrieved information

### Maryland Property Data
- Retrieves property data from the Maryland property database
- Maps property fields like block/lot numbers, sale dates, property dimensions, and assessed values
- Creates automatic links to map locations and property records
- Identifies vacant lots based on improvement values
- Processes address information to generate standardized hundred-block values

### Common Features
- Supports batch processing of multiple addresses
- Multi-threaded processing for improved performance
- Robust error handling and automatic retries
- Batch updates to Google Sheets to reduce API calls
- Detailed logging for troubleshooting
- Configurable processing with command-line options
- Options for testing single addresses or account numbers

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
   IMPERSONATED_USER = "your-user@mountwilsoncapital.com"  # Replace with your actual email
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
python src/main.py --list-sheets
```

If you still have issues, check the log file `baltimore.log` for more detailed error messages.

## Usage

### Preparing Your Spreadsheet

#### Water Bill Spreadsheet
1. Create a sheet with the following structure:
   - Column A: Service addresses to look up
   - Columns B-I will be populated with the results (account number, bill date, amounts, etc.)

#### Property Data Spreadsheet
1. Create a sheet named "LIENS" (or customize the name in settings)
   - Column A: Property addresses to look up
   - Other columns will be populated with property data (block, lot, sale dates, etc.)

2. Share the spreadsheet with your service account email address

### Running the Script

#### Working with Multiple Sheets

To list all available sheets in the spreadsheet:

```
python src/main.py --list-sheets
```

To process water bill data only:

```
python src/main.py --mode water
```

To process property data only:

```
python src/main.py --mode property
```

To process both water bill and property data:

```
python src/main.py --mode both
```

To process a specific sheet:

```
python src/main.py --mode water --sheet "MyWaterBillSheet"
```

To change the delay between requests:

```
python src/main.py --delay 5.0
```

### Additional Command-Line Options

```
python src/main.py --start-row 10 --stop-row 50  # Process only rows 10-50
python src/main.py --max-rows 100  # Process at most 100 rows
python src/main.py --skip-rows "5,8,10-15"  # Skip specific rows or ranges
```

### Testing

To test with a single address:

```
python src/main.py --address "1513 ABBOTSTON ST"
```

To test with a single account number:

```
python src/main.py --account "11000172386"
```

## File Structure

- `src/main.py`: Main execution script
- `src/scraper.py`: Water bill web scraping functionality
- `src/property_api.py`: Maryland property data API client
- `src/sheets.py`: Google Sheets integration
- `src/config.py`: Configuration settings
- `secrets/`: Directory for credentials (not in version control)
  - `google_credentials.py`: Imported credentials
  - `service_account.json`: Service account credentials

## Requirements

See `requirements.txt` for the complete list of dependencies:

```
requests>=2.25.1
beautifulsoup4>=4.9.3
google-api-python-client>=2.0.0
google-auth>=2.0.0
google-auth-httplib2>=0.1.0
google-auth-oauthlib>=0.4.0
pydantic>=2.0.0
pydantic-settings>=2.0.0
python-dotenv>=0.19.0
```
