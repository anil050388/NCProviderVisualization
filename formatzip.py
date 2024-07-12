import pandas as pd
from uszipcode import SearchEngine

# Initialize the ZIP code search engine
search = SearchEngine()

# Function to format ZIP codes
def format_zip_code(zip_code):
    zip_code = str(zip_code)
    if pd.isnull(zip_code) or zip_code == '' or zip_code.lower() == 'nan':
        return 'review'
    if len(zip_code) == 9 and zip_code.isdigit():
        return f"{zip_code[:5]}-{zip_code[5:]}"
    elif len(zip_code) == 5 or (len(zip_code) == 10 and '-' in zip_code):
        return zip_code
    else:
        return 'review'

# Function to get city, county, and state name with fallback mechanism
def get_location_info(zip_code):
    if zip_code == 'review':
        return pd.Series({'City': 'review', 'County': 'review', 'State': 'review'})
    
    try:
        # Extract the base ZIP code
        base_zip_code = zip_code.split('-')[0] if '-' in zip_code else zip_code
        
        # Retrieve location information
        zipcode_info = search.by_zipcode(base_zip_code)
        if zipcode_info:
            return pd.Series({
                'City': zipcode_info.major_city,
                'County': zipcode_info.county,
                'State': zipcode_info.state
            })
        else:
            return pd.Series({'City': 'review', 'County': 'review', 'State': 'review'})
    except Exception as e:
        print(f"Error fetching info for ZIP code {zip_code}: {str(e)}")
        return pd.Series({'City': 'error', 'County': 'error', 'State': 'error'})

# Path to the local filtered CSV file
local_filtered_csv_path = 'Query_Taxonomy_Code.csv'

# Read the CSV file from the local file system
df = pd.read_csv(local_filtered_csv_path, low_memory=False)

# Process the 'Provider Business Practice Location Address Postal Code' column
df['Formatted Postal Code'] = df['provider business practice location address postal code'].apply(format_zip_code)

# Drop rows where 'Formatted Postal Code' is 'review'
df_filtered = df[df['Formatted Postal Code'] != 'review']

# Get city, county, and state name for each 'Formatted Postal Code'
location_info = df_filtered['Formatted Postal Code'].apply(get_location_info)
df_filtered = pd.concat([df_filtered, location_info], axis=1)

# Save the DataFrame with the new columns to a local CSV file
local_file_path = 'Formatted_Query_Taxonomy_Code_Review.csv'
df_filtered.to_csv(local_file_path, index=False)

print(f"Filtered data with location info saved to {local_file_path}")
