import pandas as pd
import os
from pathlib import Path


# ============== CONFIGURATION ==============
# Set these values once before calling process_etof_file
# All existing calls will automatically use these settings
SHIPPER_ID = None  # e.g., "iffdgf" or "apple"
MISMATCH_REPORT_PATHS = None  # e.g., "report.xlsx" or ["report1.xlsx", "report2.xlsx"]
# ===========================================


def configure_enrichment(shipper_id, mismatch_report_paths):
    """
    Configure the enrichment settings for process_etof_file.
    Call this once at the start, and all calls to process_etof_file will use these settings.
    
    Args:
        shipper_id (str): Shipper identifier (e.g., "iffdgf", "apple")
        mismatch_report_paths (str or list): Single path or list of paths to mismatch_report xlsx files
    """
    global SHIPPER_ID, MISMATCH_REPORT_PATHS
    SHIPPER_ID = shipper_id
    MISMATCH_REPORT_PATHS = mismatch_report_paths


def save_dataframe_to_excel(df, output_filename, folder_name="partly_df"):
    output_folder = Path(__file__).parent / folder_name
    output_folder.mkdir(exist_ok=True)
    df.to_excel(output_folder / output_filename, index=False, engine='openpyxl')


def process_etof_file(file_path):
    """
    Process an ETOF Excel file from the input folder.
    
    Enrichment is automatically applied based on the global configuration.
    Call configure_enrichment() once before using this function to enable enrichment.
    
    Args:
        file_path (str): Path to the file relative to the "input/" folder (e.g., "etof_file.xlsx")
    
    Returns:
        tuple: (dataframe, list of column names)
            - dataframe: Processed pandas DataFrame with specified columns removed
            - list: List of column names in the processed dataframe
    """
    # Use global configuration
    shipper_id = SHIPPER_ID
    mismatch_report_paths = MISMATCH_REPORT_PATHS
    
    # Construct full path from input folder
    input_folder = "input"
    full_path = os.path.join(input_folder, file_path)
    
    # Read Excel file (skip first row)
    df_etofs = pd.read_excel(full_path, skiprows=1)
    
    # Rename duplicate columns
    new_column_names = {
        'Country code': 'Origin Country',
        'Postal code': 'Origin Postal Code',
        'Airport': 'Origin Airport',
        'City': 'Origin City',
        'Country code.1': 'Destination Country',
        'Postal code.1': 'Destination Postal Code',
        'Airport.1': 'Destination Airport',
        'City.1': 'Destination City',
        'Seaport': 'Origin Seaport',
        'Seaport.1': 'Destination Seaport'
    }
    df_etofs = df_etofs.rename(columns=new_column_names, inplace=False)
    

    columns_to_remove = ['Match', 'Approve', 'Calculation', 'State', 'Issue', 'Carrier agreement #',
                         'Currency', 'Value', 'Currency.1', 'Value.1', 'Currency.2', 'Value.2']
    # Remove specified columns
    # Only remove columns that actually exist in the dataframe
    columns_to_drop = [col for col in columns_to_remove if col in df_etofs.columns]
    if columns_to_drop:
        df_etofs = df_etofs.drop(columns=columns_to_drop)
    
    # Get list of column names
    column_names = df_etofs.columns.tolist()

    def extract_country_code(country_string):
        """Extract the two-letter country code from a country string."""
        if isinstance(country_string, str) and ' - ' in country_string:
            return country_string.split(' - ')[0]
        return country_string

    df_etofs['Origin Country'] = df_etofs['Origin Country'].apply(extract_country_code)
    df_etofs['Destination Country'] = df_etofs['Destination Country'].apply(extract_country_code)

    # Apply enrichments if shipper_id and mismatch_report_paths are provided
    if shipper_id is not None and mismatch_report_paths is not None:
        # Enrich with SHIPMENT_ID (for shipper 'iffdgf')
        df_etofs = enrich_etof_with_shipment_id(df_etofs, shipper_id, mismatch_report_paths)
        
        # Enrich SERVICE column (for shipper 'apple')
        df_etofs = enrich_etof_with_service(df_etofs, shipper_id, mismatch_report_paths)
        
        # Update column names after enrichment
        column_names = df_etofs.columns.tolist()

    return df_etofs, column_names


def load_mismatch_reports(mismatch_report_paths):
    """
    Load and combine one or multiple mismatch report files.
    
    Args:
        mismatch_report_paths (str or list): Single path or list of paths to mismatch_report xlsx files
                                              relative to "input/" folder
    
    Returns:
        pd.DataFrame: Combined dataframe from all mismatch reports
    """
    input_folder = "input"
    
    # Normalize to list
    if isinstance(mismatch_report_paths, str):
        mismatch_report_paths = [mismatch_report_paths]
    
    # Read and combine all mismatch reports
    dfs = []
    for path in mismatch_report_paths:
        full_path = os.path.join(input_folder, path)
        df = pd.read_excel(full_path)
        dfs.append(df)
    
    # Concatenate all dataframes
    df_combined = pd.concat(dfs, ignore_index=True)
    
    return df_combined


def enrich_etof_with_shipment_id(df_etofs, shipper_id, mismatch_report_paths):
    """
    Enrich ETOF dataframe with SHIPMENT_ID from mismatch_report file(s).
    
    Args:
        df_etofs (pd.DataFrame): The processed ETOF dataframe
        shipper_id (str): Mandatory shipper identifier (e.g., "iffdgf")
        mismatch_report_paths (str or list): Single path or list of paths to mismatch_report xlsx files
                                              relative to "input/" folder
    
    Returns:
        pd.DataFrame: The enriched dataframe with SHIPMENT_ID column (if applicable)
    """
    # Only process for shipper_id 'iffdgf'
    if shipper_id.lower() != 'iffdgf':
        return df_etofs
    
    # Check if SHIPMENT_ID column is already present and has non-empty values
    if 'SHIPMENT_ID' in df_etofs.columns:
        # Return unchanged only if column exists AND has at least one non-empty value
        if df_etofs['SHIPMENT_ID'].notna().any() and (df_etofs['SHIPMENT_ID'].astype(str).str.strip() != '').any():
            return df_etofs
    
    # Load and combine mismatch report(s)
    df_mismatch = load_mismatch_reports(mismatch_report_paths)
    
    # Create a mapping from ETOF_NUMBER to SHIPMENT_ID
    etof_to_shipment_mapping = dict(zip(
        df_mismatch['ETOF_NUMBER'].astype(str),
        df_mismatch['SHIPMENT_ID']
    ))
    
    # Add SHIPMENT_ID column by mapping ETOF # values
    df_etofs['SHIPMENT_ID'] = df_etofs['ETOF #'].astype(str).map(etof_to_shipment_mapping)
    
    return df_etofs


def enrich_etof_with_service(df_etofs, shipper_id, mismatch_report_paths):
    """
    Enrich ETOF dataframe by rewriting SERVICE column from mismatch_report file(s).
    
    Args:
        df_etofs (pd.DataFrame): The processed ETOF dataframe
        shipper_id (str): Mandatory shipper identifier (e.g., "apple")
        mismatch_report_paths (str or list): Single path or list of paths to mismatch_report xlsx files
                                              relative to "input/" folder
    
    Returns:
        pd.DataFrame: The enriched dataframe with updated SERVICE column (if applicable)
    """
    # Only process for shipper_id 'apple'
    if shipper_id.lower() != 'apple':
        return df_etofs
    
    # Check if SERVICE column exists in df_etofs
    if 'Service' not in df_etofs.columns:
        return df_etofs
    
    # Load and combine mismatch report(s)
    df_mismatch = load_mismatch_reports(mismatch_report_paths)
    
    # Check if SERVICE_ISD column exists in mismatch_report
    if 'SERVICE_ISD' not in df_mismatch.columns:
        return df_etofs
    
    # Create a mapping from ETOF_NUMBER to SERVICE_ISD
    etof_to_service_mapping = dict(zip(
        df_mismatch['ETOF_NUMBER'].astype(str),
        df_mismatch['SERVICE_ISD']
    ))
    
    # Map ETOF # to SERVICE_ISD, keep original Service value if ETOF # not found in mismatch report
    mapped_service = df_etofs['ETOF #'].astype(str).map(etof_to_service_mapping)
    df_etofs['Service'] = mapped_service.fillna(df_etofs['Service'])
    
    return df_etofs


#if __name__ == "__main__":
    # Configure enrichment ONCE at the start
    # All calls to process_etof_file will automatically use these settings
    #configure_enrichment(
        #shipper_id="apple",  # or "iffdgf"
        #mismatch_report_paths="mismatch_report_apple.xlsx"
        # Or multiple: ["Mismatch_Report_1.xlsx", "Mismatch_Report_2.xlsx"]
    #)
    
    # Now all existing calls work with enrichment - NO CHANGES NEEDED
    #etof_dataframe, etof_column_names = process_etof_file('etofs_apple.xlsx')
    
   # save_dataframe_to_excel(etof_dataframe, "etof_processed_apple.xlsx")
   # print(etof_dataframe.head())

