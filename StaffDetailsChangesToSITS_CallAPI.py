import os
import glob
import logging
import codecs
import xml.etree.ElementTree as ET
import requests
from requests.auth import HTTPBasicAuth
import json
#sqlbusapps StaffDetailsChangesToSITS_CallAPI.py
# Set up logging
log_file = r'\\sqlbusapps.rave.ac.uk\d$\To_SITS\Log\StaffDetailsChanges_APICalls.txt'
log_dir = os.path.dirname(log_file)

# Input file path
input_file = r'\\sqlbusapps.rave.ac.uk\d$\To_SITS\StaffDetails\StaffDetailsChanges_ZNBWS.xml'

# API details
api_url = 'https://evisiondev.nonprod.ravensbourne.tribalsits.com/urd/sits.urd/run/SIW_RWS/PERSON'
api_url = 'https://evision.prod.ravensbourne.tribalsits.com/urd/sits.urd/run/SIW_RWS/PERSON'
api_username = 'STUTALK-AD'
api_password = 'sV%5wr\\0L06('

# Ensure the log directory exists
try:
    os.makedirs(log_dir, exist_ok=True)
except Exception as e:
    print(f"Error creating log directory: {e}", file=sys.stderr)
    sys.exit(1)

# Set up logging
try:
    logging.basicConfig(filename=log_file, level=logging.INFO, 
                        format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logging.info("Logging initialized")
except Exception as e:
    print(f"Error setting up logging: {e}", file=sys.stderr)
    sys.exit(1)

def parse_xml(xml_string):
    try:
        root = ET.fromstring(xml_string)
        personnel_code = root.findtext('.//PersonnelCode', '')
        return personnel_code
    except ET.ParseError as e:
        logging.error(f"XML parsing error: {e}")
        return None

def call_api(data, person_code):
    params = {
        'DATA': data,
        'PERSON_CODE': person_code
    }
    try:
        response = requests.post(
            api_url,
            data=params,  # Use data instead of params for POST
            auth=HTTPBasicAuth(api_username, api_password)
        )
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"API call error: {e}")
        return None

# Log start of processing
logging.info(f"Starting processing of {input_file}")
print(f"Starting processing of {input_file}")

try:
    with open(input_file, 'r') as file:
        for line_number, line in enumerate(file, 1):
            line = line.strip()
            print(f"Line content: {line}")
            if line:
                logging.info(f"Processing line {line_number}")
                person_code = parse_xml(line)
                if person_code:
                    logging.info(f"PersonnelCode found: {person_code}")
                    api_response = call_api(line, person_code)
                    if api_response:
                        logging.info(f"API Response for line {line_number}: {api_response[:200]}...")  # Log first 200 chars
                    else:
                        logging.warning(f"No API response for line {line_number}")
                else:
                    logging.warning(f"Could not parse PersonnelCode in line {line_number}")
            else:
                logging.info(f"Skipping empty line {line_number}")

except FileNotFoundError:
    error_msg = f"Error: Input file '{input_file}' not found."
    print(error_msg, file=sys.stderr)
    logging.error(error_msg)
except Exception as e:
    error_msg = f"An unexpected error occurred: {e}"
    print(error_msg, file=sys.stderr)
    logging.error(error_msg)

# Log end of processing
logging.info("Processing complete.")
print(f"Processing complete. Log file saved at: {log_file}")

# Verify log file contents
try:
    with open(log_file, 'r') as f:
        log_contents = f.read()
    print(f"Log file contents (first 500 characters):\n{log_contents[:500]}")
except Exception as e:
    print(f"Error reading log file: {e}", file=sys.stderr)