import pandas as pd
import os
import difflib
from pathlib import Path
from part5_order_files_export_processing import process_order_files_export
from part2_lc_processing import process_lc_input
from part1_etof_file_processing import process_etof_file


def save_dataframe_to_excel(df, output_filename, folder_name="partly_df"):
    output_folder = Path(__file__).parent / folder_name
    output_folder.mkdir(exist_ok=True)
    df.to_excel(output_folder / output_filename, index=False, engine='openpyxl')


def fuzzy_match_filename(filename, order_file_names):
    """
    Try to find the best match for filename in order_file_names.
    Matching is case-insensitive and ignores file extensions.
    
    Args:
        filename: The filename to match
        order_file_names: List of order file names to match against
    
    Returns:
        The matched order file name from order_file_names if found, else None.
    """
    def normalize(f):
        return os.path.splitext(os.path.basename(str(f)).lower().strip())[0]
    
    filename_norm = normalize(filename)
    order_file_names_norm = [normalize(name) for name in order_file_names]
    
    # First try exact match
    if filename_norm in order_file_names_norm:
        idx = order_file_names_norm.index(filename_norm)
        return order_file_names[idx]
    
    # Then try fuzzy match
    matches = difflib.get_close_matches(filename_norm, order_file_names_norm, n=1, cutoff=0.7)
    if matches:
        idx = order_file_names_norm.index(matches[0])
        return order_file_names[idx]
    else:
        return None


def map_order_file_to_lc(order_files_dataframe, lc_dataframe):
    """
    Map "Order file #" from order_files_dataframe to lc_dataframe based on matching
    "Order file name" (from order_files_dataframe) with "ORIG_FILE_NAME" (from lc_dataframe).
    
    Args:
        order_files_dataframe: DataFrame with "Order file #" and "Order file name" columns
        lc_dataframe: DataFrame with "ORIG_FILE_NAME" column (and other LC data)
    
    Returns:
        DataFrame: lc_dataframe with added "Order file #" column
    """
    # Create a copy to avoid modifying the original
    lc_dataframe_updated = lc_dataframe.copy()
    
    # Check required columns exist
    if 'Order file #' not in order_files_dataframe.columns or 'Order file name' not in order_files_dataframe.columns:
        raise ValueError("order_files_dataframe must have 'Order file #' and 'Order file name' columns")
    
    if 'ORIG_FILE_NAME' not in lc_dataframe_updated.columns:
        raise ValueError("lc_dataframe must have 'ORIG_FILE_NAME' column")
    
    # Get list of order file names for matching
    order_file_names_list = order_files_dataframe['Order file name'].astype(str).tolist()
    
    # Create mapping function
    def find_order_file_number(row):
        filename = row.get('ORIG_FILE_NAME')
        if pd.isna(filename):
            return None
        
        matched_name = fuzzy_match_filename(filename, order_file_names_list)
        if matched_name is not None:
            value = order_files_dataframe.loc[
                order_files_dataframe['Order file name'] == matched_name, 
                'Order file #'
            ]
            if not value.empty:
                return value.values[0]
        return None
    
    # Apply mapping
    lc_dataframe_updated['Order file #'] = lc_dataframe_updated.apply(find_order_file_number, axis=1)
    
    return lc_dataframe_updated


def map_etof_to_lc(etof_dataframe, lc_dataframe_updated):
    """
    Map "ETOF #" from etof_dataframe to lc_dataframe_updated.
    If SHIPMENT_ID is present in both dataframes, uses SHIPMENT_ID for mapping.
    Otherwise, uses "Order file #" (from lc_dataframe_updated) with "LC #" (from etof_dataframe).
    Also renames "Order file #" column to "LC #".
    
    Args:
        etof_dataframe: DataFrame with "ETOF #" column and optionally "LC #" and "SHIPMENT_ID" columns
        lc_dataframe_updated: DataFrame with "Order file #" column (from previous mapping) and optionally "SHIPMENT_ID"
    
    Returns:
        tuple: (dataframe, list of column names)
            - dataframe: lc_dataframe_updated with added "ETOF #" column and "Order file #" renamed to "LC #"
            - list: List of column names in the processed dataframe
    """
    # Create a copy to avoid modifying the original
    lc_dataframe_final = lc_dataframe_updated.copy()
    
    # Check required columns exist
    if 'ETOF #' not in etof_dataframe.columns:
        raise ValueError("etof_dataframe must have 'ETOF #' column")
    
    # Check if SHIPMENT_ID is present in both dataframes
    has_shipment_id_etof = 'SHIPMENT_ID' in etof_dataframe.columns
    has_shipment_id_lc = 'SHIPMENT_ID' in lc_dataframe_final.columns
    use_shipment_id = has_shipment_id_etof and has_shipment_id_lc
    
    # Check if DELIVERY_NUMBER is present in both dataframes (fallback option)
    # Find DELIVERY_NUMBER column in LC (handle variations)
    delivery_col_lc = None
    for col in ['DELIVERY_NUMBER', 'Delivery Number', 'delivery_number', 'DeliveryNumber', 'DELIVERY NUMBER']:
        if col in lc_dataframe_final.columns:
            delivery_col_lc = col
            break
    
    # Find DELIVERY NUMBER(s) column in ETOF (handle variations)
    delivery_col_etof = None
    for col in ['DELIVERY NUMBER(s)', 'DELIVERY_NUMBER(s)', 'Delivery Number(s)', 'DELIVERY NUMBER', 
                'DELIVERY_NUMBER', 'Delivery Number', 'delivery_number']:
        if col in etof_dataframe.columns:
            delivery_col_etof = col
            break
    
    use_delivery_number = delivery_col_lc is not None and delivery_col_etof is not None
    
    if use_shipment_id:
        # Use SHIPMENT_ID for mapping
        # Create mapping dictionaries: SHIPMENT_ID (from ETOF) -> ETOF # and LC # (from ETOF)
        shipment_to_etof = {}
        shipment_to_lc = {}
        for _, row in etof_dataframe.iterrows():
            shipment_id = str(row.get('SHIPMENT_ID', '')).strip()
            etof_value = str(row.get('ETOF #', '')).strip()
            lc_value = str(row.get('LC #', '')).strip() if 'LC #' in etof_dataframe.columns else None
            
            if pd.notna(row.get('SHIPMENT_ID')) and shipment_id and shipment_id.lower() != 'nan':
                if pd.notna(row.get('ETOF #')) and etof_value and etof_value.lower() != 'nan':
                    # Map SHIPMENT_ID (key) to ETOF # (value)
                    shipment_to_etof[shipment_id] = etof_value
                
                if lc_value and pd.notna(row.get('LC #')) and lc_value.lower() != 'nan':
                    # Map SHIPMENT_ID (key) to LC # (value)
                    shipment_to_lc[shipment_id] = lc_value
        
        # Map ETOF # values by matching SHIPMENT_ID
        def find_etof_number_by_shipment(row):
            shipment_id = str(row.get('SHIPMENT_ID', '')).strip()
            if pd.isna(row.get('SHIPMENT_ID')) or shipment_id == '' or shipment_id.lower() == 'nan':
                return None
            return shipment_to_etof.get(shipment_id)
        
        # Map LC # values by matching SHIPMENT_ID
        def find_lc_number_by_shipment(row):
            shipment_id = str(row.get('SHIPMENT_ID', '')).strip()
            if pd.isna(row.get('SHIPMENT_ID')) or shipment_id == '' or shipment_id.lower() == 'nan':
                return None
            return shipment_to_lc.get(shipment_id)
        
        # Apply mappings
        lc_dataframe_final['ETOF #'] = lc_dataframe_final.apply(find_etof_number_by_shipment, axis=1)
        
        # Map LC # from ETOF if available, otherwise use existing or create empty
        if shipment_to_lc:
            lc_dataframe_final['LC #'] = lc_dataframe_final.apply(find_lc_number_by_shipment, axis=1)
        elif 'Order file #' in lc_dataframe_final.columns:
            lc_dataframe_final = lc_dataframe_final.rename(columns={'Order file #': 'LC #'})
        else:
            lc_dataframe_final['LC #'] = None
    
    elif use_delivery_number:
        # Fallback: Use DELIVERY_NUMBER for mapping when SHIPMENT_ID is not available
        print(f"   Using DELIVERY_NUMBER mapping: LC column '{delivery_col_lc}' <-> ETOF column '{delivery_col_etof}'")
        
        # Create mapping dictionaries: DELIVERY_NUMBER (from ETOF) -> ETOF # and LC # (from ETOF)
        # Two levels of mapping:
        # 1. Exact match on the whole delivery number string (e.g., "2015141638  , 2015151082  , ...")
        # 2. Individual number match (e.g., "2015141638")
        
        # Exact (full string) mappings
        delivery_exact_to_etof = {}
        delivery_exact_to_lc = {}
        
        # Individual number mappings
        delivery_individual_to_etof = {}
        delivery_individual_to_lc = {}
        
        for _, row in etof_dataframe.iterrows():
            delivery_value = str(row.get(delivery_col_etof, '')).strip()
            etof_value = str(row.get('ETOF #', '')).strip()
            lc_value = str(row.get('LC #', '')).strip() if 'LC #' in etof_dataframe.columns else None
            
            if pd.notna(row.get(delivery_col_etof)) and delivery_value and delivery_value.lower() != 'nan':
                # First: Store exact (full string) mapping
                if pd.notna(row.get('ETOF #')) and etof_value and etof_value.lower() != 'nan':
                    delivery_exact_to_etof[delivery_value] = etof_value
                
                if lc_value and pd.notna(row.get('LC #')) and lc_value.lower() != 'nan':
                    delivery_exact_to_lc[delivery_value] = lc_value
                
                # Second: Store individual number mappings
                # Handle multiple delivery numbers (comma or semicolon separated, with possible spaces)
                # Example: "2015141638  , 2015151082  , 2015151083  , 2015155815"
                delivery_numbers = [d.strip() for d in delivery_value.replace(';', ',').split(',') if d.strip()]
                
                for delivery_num in delivery_numbers:
                    if delivery_num and delivery_num.lower() != 'nan':
                        if pd.notna(row.get('ETOF #')) and etof_value and etof_value.lower() != 'nan':
                            delivery_individual_to_etof[delivery_num] = etof_value
                        
                        if lc_value and pd.notna(row.get('LC #')) and lc_value.lower() != 'nan':
                            delivery_individual_to_lc[delivery_num] = lc_value
        
        print(f"   Built exact mapping with {len(delivery_exact_to_etof)} full strings -> ETOF #")
        print(f"   Built individual mapping with {len(delivery_individual_to_etof)} delivery numbers -> ETOF #")
        
        # Map ETOF # values by matching DELIVERY_NUMBER
        # Priority: 1. Exact match on full string, 2. Individual number match
        def find_etof_number_by_delivery(row):
            delivery_num = str(row.get(delivery_col_lc, '')).strip()
            if pd.isna(row.get(delivery_col_lc)) or delivery_num == '' or delivery_num.lower() == 'nan':
                return None
            
            # First try exact match on the whole string
            if delivery_num in delivery_exact_to_etof:
                return delivery_exact_to_etof[delivery_num]
            
            # Then try individual number match
            return delivery_individual_to_etof.get(delivery_num)
        
        # Map LC # values by matching DELIVERY_NUMBER
        def find_lc_number_by_delivery(row):
            delivery_num = str(row.get(delivery_col_lc, '')).strip()
            if pd.isna(row.get(delivery_col_lc)) or delivery_num == '' or delivery_num.lower() == 'nan':
                return None
            
            # First try exact match on the whole string
            if delivery_num in delivery_exact_to_lc:
                return delivery_exact_to_lc[delivery_num]
            
            # Then try individual number match
            return delivery_individual_to_lc.get(delivery_num)
        
        # Apply mappings
        lc_dataframe_final['ETOF #'] = lc_dataframe_final.apply(find_etof_number_by_delivery, axis=1)
        matched_count = lc_dataframe_final['ETOF #'].notna().sum()
        print(f"   Mapped {matched_count} rows using DELIVERY_NUMBER")
        
        # Map LC # from ETOF if available, otherwise use existing or create empty
        if delivery_exact_to_lc or delivery_individual_to_lc:
            lc_dataframe_final['LC #'] = lc_dataframe_final.apply(find_lc_number_by_delivery, axis=1)
        elif 'Order file #' in lc_dataframe_final.columns:
            lc_dataframe_final = lc_dataframe_final.rename(columns={'Order file #': 'LC #'})
        elif 'LC #' not in lc_dataframe_final.columns:
            lc_dataframe_final['LC #'] = None
    
    else:
        # Fall back to LC # matching (original method) - requires Order file #
        if 'Order file #' not in lc_dataframe_final.columns:
            raise ValueError("lc_dataframe_updated must have 'Order file #' column when SHIPMENT_ID and DELIVERY_NUMBER are not available")
        
        if 'LC #' not in etof_dataframe.columns:
            raise ValueError("etof_dataframe must have 'LC #' column when SHIPMENT_ID and DELIVERY_NUMBER are not available")
        
        # Create mapping dictionary: LC # (from ETOF) -> ETOF # (from ETOF)
        lc_to_etof = {}
        for _, row in etof_dataframe.iterrows():
            lc_value = str(row.get('LC #', '')).strip()
            etof_value = str(row.get('ETOF #', '')).strip()
            if pd.notna(row.get('LC #')) and lc_value and lc_value.lower() != 'nan':
                if pd.notna(row.get('ETOF #')) and etof_value and etof_value.lower() != 'nan':
                    # Map LC # (key) to ETOF # (value)
                    lc_to_etof[lc_value] = etof_value
        
        # Map ETOF # values by matching Order file # from LC dataframe with LC # from ETOF file
        def find_etof_number_by_lc(row):
            order_file_number = str(row.get('Order file #', '')).strip()
            if pd.isna(row.get('Order file #')) or order_file_number == '' or order_file_number.lower() == 'nan':
                return None
            # Match Order file # with LC # from ETOF file, return corresponding ETOF #
            return lc_to_etof.get(order_file_number)
        
        # Apply mapping
        lc_dataframe_final['ETOF #'] = lc_dataframe_final.apply(find_etof_number_by_lc, axis=1)
        
        # Rename "Order file #" to "LC #"
        lc_dataframe_final = lc_dataframe_final.rename(columns={'Order file #': 'LC #'})
    
    # Get list of column names
    column_names = lc_dataframe_final.columns.tolist()
    
    return lc_dataframe_final, column_names


def process_order_lc_mapping(order_files_path, lc_input_path, lc_recursive=False):
    """
    Complete workflow: Process order files export and LC files, then map Order file # to LC dataframe.
    
    Args:
        order_files_path (str): Path to order files export file relative to "input/" folder
        lc_input_path (str or list): Path(s) to LC file(s) or folder(s) relative to "input/" folder
        lc_recursive (bool): Whether to search recursively in LC folders (default: False)
    
    Returns:
        DataFrame: LC dataframe with added "Order file #" column
    """
    # Process order files export
    order_files_dataframe = process_order_files_export(order_files_path)
    
    # Process LC files
    lc_dataframe, lc_column_names = process_lc_input(lc_input_path, recursive=lc_recursive)
    
    # Map Order file # to LC dataframe
    lc_dataframe_updated = map_order_file_to_lc(order_files_dataframe, lc_dataframe)
    
    save_dataframe_to_excel(lc_dataframe_updated, "order_lc_mapping.xlsx")
    
    return lc_dataframe_updated


def process_order_lc_etof_mapping(lc_input_path, etof_path, order_files_path=None, lc_recursive=False):
    """
    Complete workflow: Process LC files and ETOF file, with optional order files export.
    
    If order_files_path is provided:
        - Maps Order file # to LC dataframe first
        - Then maps ETOF # using LC # matching or SHIPMENT_ID
        - Renames Order file # to LC #
    
    If order_files_path is NOT provided:
        - Maps ETOF # to LC dataframe directly using SHIPMENT_ID
        - Creates empty LC # column if needed
    
    Args:
        lc_input_path (str or list): Path(s) to LC file(s) or folder(s) relative to "input/" folder
        etof_path (str): Path to ETOF file relative to "input/" folder
        order_files_path (str, optional): Path to order files export file relative to "input/" folder
        lc_recursive (bool): Whether to search recursively in LC folders (default: False)
    
    Returns:
        tuple: (dataframe, list of column names)
            - dataframe: LC dataframe with "LC #" and "ETOF #" columns
            - list: List of column names in the processed dataframe
    """
    # Step 1: Process LC files
    lc_dataframe, lc_column_names = process_lc_input(lc_input_path, recursive=lc_recursive)
    
    # Step 2: If order_files_path is provided, map Order file # first
    if order_files_path:
        lc_dataframe = map_order_file_to_lc(
            process_order_files_export(order_files_path), 
            lc_dataframe
        )
        output_filename = "order_lc_etof_mapping.xlsx"
    else:
        output_filename = "lc_etof_mapping.xlsx"
    
    # Step 3: Process ETOF file
    etof_dataframe, etof_column_names = process_etof_file(etof_path)
    
    # Step 4: Map ETOF # to LC dataframe
    lc_dataframe_final, lc_column_names = map_etof_to_lc(etof_dataframe, lc_dataframe)
    
    save_dataframe_to_excel(lc_dataframe_final, output_filename)
    
    return lc_dataframe_final, lc_column_names


#if __name__ == "__main__":
    #lc_input_path = "lc_dairb.xml"
    #etof_path = "etofs_dairb.xlsx"
    
    # If order_files_path is provided, it will use order file mapping logic
    # If not provided (None), it will use SHIPMENT_ID mapping
#    order_files_path = "Order_files_export.xls.xlsx"  # Set to None or omit to use SHIPMENT_ID mapping
    
    #df_lc_updated, lc_column_names = process_order_lc_etof_mapping(
       # lc_input_path, 
      #  etof_path, 
        #order_files_path=order_files_path
#    )

