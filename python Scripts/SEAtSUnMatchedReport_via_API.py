import requests
import csv
import pandas as pd
import json
import os
import sys
from datetime import datetime, timedelta

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
        response.raise_for_status()
        data = response.json()
        bearer_token = data.get('access_token')
        token_type = data.get('token_type', '').lower()
        
        if bearer_token and token_type == 'bearer':
            return bearer_token
        else:
            print("Error: Failed to retrieve bearer token.")
            return None
    except requests.RequestException as e:
        print(f"An error occurred while fetching the token: {e}")
        return None

# Function to convert date format to YYYY-MM-DD
def convert_date_format(date_str):
    if not date_str:
        return date_str
    
    try:
        # Handle dd/mm/yyyy hh:mm:ss format
        if '/' in date_str:
            # First try with time portion
            try:
                return datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d")
            except ValueError:
                # If that fails, try without time portion
                return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        return date_str  # Return as is if already in desired format
    except ValueError:
        print(f"Warning: Could not parse date '{date_str}'")
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
                    next_url = pagination_data.get('NextPageLink')
                    
                    # Ensure next_url is valid
                    if next_url and next_url.startswith("http"):
                        url = next_url
                    else:
                        url = None
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
def write_data_to_csv(data, start_date, end_date):
    if data:
        raw_file_path = rf"\\sqlbusapps.rave.ac.uk\d$\From_SEAtS\STUM_{start_date}_{end_date}.csv"
        second_raw_file_path = rf"\\sqlbusapps.rave.ac.uk\d$\From_SEAtS\STUM.csv"  # New second raw file

        try:
            with open(raw_file_path, mode='w', newline='', encoding='utf-8') as file1, \
                 open(second_raw_file_path, mode='w', newline='', encoding='utf-8') as file2:

                fieldnames = data[0].keys()
                writer1 = csv.DictWriter(file1, fieldnames=fieldnames, quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer2 = csv.DictWriter(file2, fieldnames=fieldnames, quotechar='"', quoting=csv.QUOTE_MINIMAL)

                writer1.writeheader()
                writer2.writeheader()

                for row in data:
                    # Convert date to YYYY-MM-DD format
                    if 'date' in row and row['date']:
                        row['date'] = convert_date_format(row['date'])
                    if 'group' in row and row['group']:
                        row['group'] = row['group'].replace(',', '.')
                    if 'room' in row and row['room']:
                        row['room'] = row['room'].replace(',', '.')
                    if 'roomName' in row and row['roomName']:
                        row['roomName'] = row['roomName'].replace(',', '.')

                    writer1.writerow(row)
                    writer2.writerow(row)

                print(f"Raw data written to {raw_file_path}")
                print(f"Second raw file written to {second_raw_file_path}")

        except Exception as e:
            print(f"Failed to write to remote location: {e}")

# Function to clean and transform the data
def clean_and_transform_csv(start_date, end_date):
    input_path = rf"\\sqlbusapps.rave.ac.uk\d$\From_SEAtS\STUM_{start_date}_{end_date}.csv"
    output_path = rf"\\sqlbusapps.rave.ac.uk\d$\From_SEAtS\STUM_CLEANED_{start_date}_{end_date}.csv"
    second_output_path = rf"\\sqlbusapps.rave.ac.uk\d$\From_SEAtS\STUM_CLEANED.csv"  # Second cleaned file

    try:
        df = pd.read_csv(input_path, dtype=str)

        column_mapping = {
            'studentNumber': 'Student Number',
            'studentName': 'studentName',
            'type': 'Type',
            'roomName': 'Room',
            'moduleCode': 'Module',
            'lecturer': 'Lecturer',
            'date': 'Date',
            'startTime': 'Start Time',
            'swipeTime': 'Swipe Time',            
            'comment': 'Comment'
        }

        df = df[list(column_mapping.keys())]
        df.rename(columns=column_mapping, inplace=True)

        # Convert date field to YYYY-MM-DD format
        if 'Date' in df.columns:
            df['Date'] = df['Date'].apply(lambda x: convert_date_format(x) if pd.notna(x) else x)

        # Remove MandatoryOptional column if it exists
        if 'MandatoryOptional' in df.columns:
            df.drop(columns=['MandatoryOptional'], inplace=True)

        # Write the cleaned data to files
        df.to_csv(output_path, index=False, encoding='utf-8', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        print(f"Cleaned data written to {output_path}")

        df.to_csv(second_output_path, index=False, encoding='utf-8', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        print(f"Second cleaned data file written to {second_output_path}")

    except Exception as e:
        print(f"Failed to clean and transform CSV: {e}")

# Main logic
def main():
    # Set start_date to 16/09/2024
    start_date_dt = datetime(2024, 9, 16)
    start_date = start_date_dt.strftime("%Y%m%d")
    end_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")

    token = get_bearer_token()
    if not token:
        print("Failed to retrieve bearer token. Exiting.")
        return
    
    base_url = "https://01clientapi.seats.cloud/api/v1/report/Attendance/"
    params = {
        'reportName': 'Student Unmatched Report',
        'dateFrom': start_date_dt.strftime("%d/%m/%Y"),
        'dateTo': (datetime.today() - timedelta(days=1)).strftime("%d/%m/%Y"),
        'IsMandatory': 'true',
        'pageNumber': 0,
        'pageSize': 1000
    }
    
    initial_url = f"{base_url}?reportName={params['reportName']}&dateFrom={params['dateFrom']}&dateTo={params['dateTo']}&IsMandatory={params['IsMandatory']}&pageNumber={params['pageNumber']}&pageSize={params['pageSize']}"
    print(f"Fetching data from: {initial_url}")

    all_data = fetch_data_with_pagination(initial_url, token)
    if all_data:
        write_data_to_csv(all_data, start_date, end_date)
        clean_and_transform_csv(start_date, end_date)
    else:
        print("No data retrieved. Exiting.")

if __name__ == "__main__":
    main()