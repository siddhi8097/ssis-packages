# This works Thurs 20th Feb
# Author Richard Fisher
# SEAtS Attendance Data Fetcher
# 20/03/2025 RFisher - Removed the Mandatory filter from the api call
# Can be true or false, but if omitted gets you both.
# Drop ins are marked as non-mandatory and so were being missed
# This code calls the SEAtS API and runs the STudent Attendance Report for the previous days
# Only students who are marked as IN get written to the CLEANED csv files.
# This is then loaded into a table into the data warehouse where it will be compared with their SITS status.
# Students whose status is RR RE RPR will be added to the XML file for uploading into SITS where# tehir status will be changed to CR
#

import requests
import csv
import pandas as pd
import os
import json
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
                    next_url = pagination_data.get('NextPageLink')

                    # Ensure next_url is a valid URL
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
        raw_file_path = rf"\\sqlbusapps.rave.ac.uk\d$\From_SEAtS\STAR_YESTERDAY_{start_date}_{end_date}.csv"
        second_raw_file_path = rf"\\sqlbusapps.rave.ac.uk\d$\From_SEAtS\STAR_YESTERDAY.csv"  # New second raw file

        try:
            with open(raw_file_path, mode='w', newline='', encoding='utf-8') as file1, \
                 open(second_raw_file_path, mode='w', newline='', encoding='utf-8') as file2:

                fieldnames = data[0].keys()
                writer1 = csv.DictWriter(file1, fieldnames=fieldnames, quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer2 = csv.DictWriter(file2, fieldnames=fieldnames, quotechar='"', quoting=csv.QUOTE_MINIMAL)

                writer1.writeheader()
                writer2.writeheader()

                # Write the rows with date conversion and group field adjustment
                for row in data:
                    if 'date' in row and row['date']:
                        row['date'] = convert_date_format(row['date'])
                    if 'group' in row and row['group']:
                        row['group'] = row['group'].replace(',', '.')

                    writer1.writerow(row)
                    writer2.writerow(row)

                print(f"Raw data written to {raw_file_path}")
                print(f"Second raw file written to {second_raw_file_path}")

        except Exception as e:
            print(f"Failed to write to remote location: {e}")

# Function to clean and transform the data
def clean_and_transform_csv(start_date, end_date):
    input_path = rf"\\sqlbusapps.rave.ac.uk\d$\From_SEAtS\STAR_YESTERDAY_{start_date}_{end_date}.csv"
    output_path = rf"\\sqlbusapps.rave.ac.uk\d$\From_SEAtS\STAR_YESTERDAY_CLEANED_{start_date}_{end_date}.csv"
    second_output_path = rf"\\sqlbusapps.rave.ac.uk\d$\From_SEAtS\STAR_YESTERDAY_CLEANED.csv"  # Second cleaned file

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

        df.rename(columns=column_mapping, inplace=True)

        # Filter only rows where Type == 'IN'
        df = df[df['Type'] == 'IN']

        df['MandatoryOptional'] = 'M'
        
        # Convert date field to YYYY-MM-DD format
        if 'date' in df.columns:
            df['date'] = df['date'].apply(lambda x: convert_date_format(x, input_format="%d/%m/%Y", output_format="%Y-%m-%d") if pd.notna(x) else x)
        
        # Add yesterday's date in the first column
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        df.insert(0, 'Processed Date', yesterday)

        # Write the first cleaned file
        df.to_csv(output_path, index=False, encoding='utf-8', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        print(f"Cleaned data written to {output_path}")

        # Write the second cleaned file
        df.to_csv(second_output_path, index=False, encoding='utf-8', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        print(f"Second cleaned data file written to {second_output_path}")

    except Exception as e:
        print(f"Failed to clean and transform CSV: {e}")

if __name__ == "__main__":
    main()
