# Accept parameters
import requests
import csv
import pandas as pd
import os
import json
import sys
from datetime import datetime

# Function to fetch the bearer token
def get_bearer_token():
    url = "https://01clientapi.seats.cloud/token"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'Key': 'PNFozLR4CYJUBfTJZ0a-mx-uYzU!i?qd5aZWzPWCRquNJmrXvGPp@9Ucec1kyCf7ck',
        'grant_type': 'authorization_key'
    }
    
    try:
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            data = response.json()
            bearer_token = data.get('access_token')
            token_type = data.get('token_type', '').lower()
            if bearer_token and token_type == 'bearer':
                return bearer_token
        else:
            print(f"Error: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Function to convert date format to YYYY-MM-DD
def convert_date_format(date_str, input_format="%d/%m/%Y", output_format="%Y-%m-%d"):
    try:
        return datetime.strptime(date_str, input_format).strftime(output_format)
    except ValueError:
        return date_str  # Return as is if conversion fails

# Function to fetch data with pagination
def fetch_data_with_pagination(url, token):
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json;charset=UTF-8'
    }
    
    all_data = []
    while url:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            all_data.extend(data)

            next_page = response.headers.get('x-pagination')
            if next_page:
                try:
                    pagination_data = json.loads(next_page)
                    url = pagination_data.get('NextPageLink', None)
                except json.JSONDecodeError:
                    print("Error decoding pagination header.")
                    url = None
            else:
                url = None
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            break

    return all_data

# Function to write raw data to CSV
def write_data_to_csv(data):
    if data:
        remote_path = r"\\sqlbusapps.rave.ac.uk\d$\From_SEAtS\STAR.csv"
        
        try:
            with open(remote_path, mode='w', newline='', encoding='utf-8') as file:
                fieldnames = data[0].keys()
                writer = csv.DictWriter(file, fieldnames=fieldnames, quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()

                # Write the rows with the date in YYYY-MM-DD format and replace commas in 'Group' field
                for row in data:
                    if 'date' in row and row['date']:
                        row['date'] = convert_date_format(row['date'])
                    if 'group' in row and row['group']:
                        row['group'] = row['group'].replace(',', '.')
                    writer.writerow(row)
                
                print(f"Raw data written to {remote_path}")
        except Exception as e:
            print(f"Failed to write to remote location: {e}")

# Function to clean and transform the data
def clean_and_transform_csv():
    input_path = r"\\sqlbusapps.rave.ac.uk\d$\From_SEAtS\STAR.csv"
    output_path = r"\\sqlbusapps.rave.ac.uk\d$\From_SEAtS\STAR_CLEANED.csv"

    try:
        df = pd.read_csv(input_path, dtype=str)

        column_mapping = {
            'type': 'Type',
            'studentNumber': 'Student No',
            'firstName': 'First Name',
            'surname': 'Surname',
            'roomName': 'Room',
            'date': 'date',
            'courseCode': 'Course',
            'moduleCode': 'Module',
            'lecturer': 'Lecturer',
            'lessonTypeDescription': 'lesson type',
            'group': 'Group',
            'startTime': 'start',
            'endTime': 'end',
            'swipeTime': 'Swipe Time',
            'percentageAttended': '% Attended'
        }

        df = df[list(column_mapping.keys())]
        df.rename(columns=column_mapping, inplace=True)

        df['MandatoryOptional'] = 'M'
        
        # Convert date field to YYYY-MM-DD format for CSV (but leave API dates as DD/MM/YYYY)
        if 'date' in df.columns:
            df['date'] = df['date'].apply(lambda x: convert_date_format(x, input_format="%d/%m/%Y", output_format="%Y-%m-%d") if pd.notna(x) else x)

        # Writing cleaned data to CSV without modifying 'Group' field formatting
        df.to_csv(output_path, index=False, encoding='utf-8', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        print(f"Cleaned data written to {output_path}")
    except Exception as e:
        print(f"Failed to clean and transform CSV: {e}")

# Function to prompt for report parameters
def get_report_parameters():
    date_from = input("Enter the start date for the report (format: DD/MM/YYYY): ")
    date_to = input("Enter the end date for the report (format: DD/MM/YYYY): ")
    
    # API call requires dates in DD/MM/YYYY format, so we leave them as-is
    is_mandatory = 'true'
    return date_from, date_to, is_mandatory

# Main logic


def main():
    # Check if parameters are passed via command line
    if len(sys.argv) == 4:
        regenerate = sys.argv[1].strip().upper()
        date_from = sys.argv[2]
        date_to = sys.argv[3]
    else:
        regenerate = input("Do you want to fetch new data from the API? (Y/N): ").strip().upper()
        if regenerate == 'Y':
            date_from = input("Enter the start date for the report (format: DD/MM/YYYY): ")
            date_to = input("Enter the end date for the report (format: DD/MM/YYYY): ")
        else:
            date_from = None
            date_to = None

    if regenerate == 'Y':
        token = get_bearer_token()
        if not token:
            print("Failed to retrieve bearer token. Exiting.")
            return
        
        if date_from and date_to:
            base_url = "https://01clientapi.seats.cloud/api/v1/report/Attendance/"
            params = {
                'reportName': 'Student Attendance Report',
                'dateFrom': date_from,  # Keep the date in DD/MM/YYYY format for API call
                'dateTo': date_to,  # Keep the date in DD/MM/YYYY format for API call
                'IsMandatory': 'true',
                'pageNumber': 1,
                'pageSize': 1000
            }
            
            initial_url = f"{base_url}?reportName={params['reportName']}&dateFrom={params['dateFrom']}&dateTo={params['dateTo']}&IsMandatory={params['IsMandatory']}&pageNumber={params['pageNumber']}&pageSize={params['pageSize']}"
            print(f"Fetching data from: {initial_url}")

            all_data = fetch_data_with_pagination(initial_url, token)
            write_data_to_csv(all_data)

    clean_and_transform_csv()

if __name__ == "__main__":
    main()

