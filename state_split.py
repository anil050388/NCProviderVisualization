import os
import pandas as pd
import re
from multiprocessing import Pool

# Define the input and output directories
input_dir = 'Final'
output_dir = 'states'

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# List of all US state abbreviations
us_states = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 
    'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 
    'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
]

# Columns to be included in the final output
selected_columns = [
    'NPI','Entity Type Code','Replacement NPI','Employer Identification Number (EIN)','Provider Organization Name (Legal Business Name)',
    'Provider Last Name (Legal Name)','Provider First Name','Provider Middle Name','Provider Name Prefix Text','Provider Name Suffix Text','Provider Credential Text','Provider Other Organization Name','Provider Other Organization Name Type Code','Provider Other Last Name','Provider Other First Name','Provider Other Middle Name','Provider Other Name Prefix Text','Provider Other Name Suffix Text','Provider Other Credential Text','Provider Other Last Name Type Code','Provider First Line Business Mailing Address','Provider Second Line Business Mailing Address','Provider Business Mailing Address City Name','Provider Business Mailing Address State Name','Provider Business Mailing Address Postal Code','Provider Business Mailing Address Country Code (If outside U.S.)','Provider Business Mailing Address Telephone Number','Provider Business Mailing Address Fax Number','Provider First Line Business Practice Location Address','Provider Second Line Business Practice Location Address','Provider Business Practice Location Address City Name','Provider Business Practice Location Address State Name','Provider Business Practice Location Address Postal Code','Provider Business Practice Location Address Country Code (If outside U.S.)','Provider Business Practice Location Address Telephone Number','Provider Business Practice Location Address Fax Number','Provider Enumeration Date','Last Update Date','NPI Deactivation Reason Code','NPI Deactivation Date','NPI Reactivation Date','Provider Gender Code','Authorized Official Last Name','Authorized Official First Name','Authorized Official Middle Name','Authorized Official Title or Position','Authorized Official Telephone Number'
]

# Function to sanitize state names to valid file names
def sanitize_filename(name):
    return "".join([c if c.isalnum() else "_" for c in name])

# Function to concatenate columns while handling null values
def concatenate_columns(df, columns):
    return df[columns].apply(lambda x: ','.join(x.dropna().astype(str).str.strip().replace('', None).dropna()), axis=1)

# Function to clean and validate postal codes
def clean_postal_code(postal_code):
    if pd.isna(postal_code):
        return None

    postal_code = str(postal_code).strip()
    
    # Check if postal code is 5 digits
    if re.fullmatch(r'\d{5}', postal_code):
        return postal_code
    
    # Check if postal code is 9 digits without hyphen, convert to ZIP+4 format
    if re.fullmatch(r'\d{9}', postal_code):
        return f"{postal_code[:5]}-{postal_code[5:]}"
    
    # Check if postal code is 9 digits with hyphen
    if re.fullmatch(r'\d{5}-\d{4}', postal_code):
        return postal_code
    
    return None

# Function to process a chunk of data
def process_chunk(chunk):
    columns_to_select = (
        selected_columns + 
        [f'Healthcare Provider Taxonomy Code_{i}' for i in range(1, 16)] + 
        [f'Provider License Number_{i}' for i in range(1, 16)] + 
        [f'Provider License Number State Code_{i}' for i in range(1, 16)] +
        [f'Other Provider Identifier_{i}' for i in range(1, 51)] +
        [f'Other Provider Identifier Type Code_{i}' for i in range(1, 51)] +
        [f'Other Provider Identifier State_{i}' for i in range(1, 51)] +
        [f'Other Provider Identifier Issuer_{i}' for i in range(1, 51)]
    )
    
    df = chunk.loc[:, columns_to_select]

    # Create concatenated fields while handling null values
    df.loc[:, 'Taxonomy Codes'] = concatenate_columns(df, [f'Healthcare Provider Taxonomy Code_{i}' for i in range(1, 16)])
    df.loc[:, 'License Numbers'] = concatenate_columns(df, [f'Provider License Number_{i}' for i in range(1, 16)])
    df.loc[:, 'State Codes'] = concatenate_columns(df, [f'Provider License Number State Code_{i}' for i in range(1, 16)])
    df.loc[:, 'Other Identifier'] = concatenate_columns(df, [f'Other Provider Identifier_{i}' for i in range(1, 51)])
    df.loc[:, 'Other Identifier Type Codes'] = concatenate_columns(df, [f'Other Provider Identifier Type Code_{i}' for i in range(1, 51)])
    df.loc[:, 'Other Identifier States'] = concatenate_columns(df, [f'Other Provider Identifier State_{i}' for i in range(1, 51)])
    df.loc[:, 'Other Identifier Issuers'] = concatenate_columns(df, [f'Other Provider Identifier Issuer_{i}' for i in range(1, 51)])

    # Drop the individual taxonomy, license, state code, and other identifier columns
    df.drop(columns=(
        [f'Healthcare Provider Taxonomy Code_{i}' for i in range(1, 16)] + 
        [f'Provider License Number_{i}' for i in range(1, 16)] + 
        [f'Provider License Number State Code_{i}' for i in range(1, 16)] +
        [f'Other Provider Identifier_{i}' for i in range(1, 51)] +
        [f'Other Provider Identifier Type Code_{i}' for i in range(1, 51)] +
        [f'Other Provider Identifier State_{i}' for i in range(1, 51)] +
        [f'Other Provider Identifier Issuer_{i}' for i in range(1, 51)]
    ), inplace=True)

    # Group the data by state
    grouped = df.groupby('Provider Business Practice Location Address State Name')

    state_data = {}
    
    # Process each state group
    for state, group in grouped:
        if state in us_states:
            sanitized_state = sanitize_filename(state)
            state_data[sanitized_state] = group

    return state_data

# Function to write state-specific files
def write_state_files(state_data):
    for state, data in state_data.items():
        state_file_path = os.path.join(output_dir, f'{state}.csv')
        
        # Append to the existing file or create a new one
        if os.path.isfile(state_file_path):
            data.to_csv(state_file_path, mode='a', header=False, index=False)
        else:
            data.to_csv(state_file_path, mode='w', header=True, index=False)

# Function to process a single CSV file
def process_csv(file_path):
    print(f"Processing file: {file_path}")
    chunk_size = 100000  # Adjust based on memory constraints and performance
    state_data_combined = {}
    
    dtype = {col: str for col in range(330)}  # Specify dtype for all columns
    
    for chunk in pd.read_csv(file_path, chunksize=chunk_size, dtype=dtype):
        state_data = process_chunk(chunk)
        
        # Combine state data from all chunks
        for state, data in state_data.items():
            if state in state_data_combined:
                state_data_combined[state] = pd.concat([state_data_combined[state], data], ignore_index=True)
            else:
                state_data_combined[state] = data
    
    # Write the combined state data to files
    write_state_files(state_data_combined)

# Main function to process all CSV files in parallel
def main():
    csv_files = [file for file in os.listdir(input_dir) if file.endswith('.csv')]
    file_paths = [os.path.join(input_dir, file) for file in csv_files]

    with Pool() as pool:
        pool.map(process_csv, file_paths)

    print("Data processing complete.")

if __name__ == "__main__":
    main()
