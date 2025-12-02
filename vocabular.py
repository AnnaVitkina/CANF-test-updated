"""
Vocabulary Mapping Script

This script:
1. Collects column lists from part4_rate_card_processing, part1_etof_file_processing, part3_origin_file_processing, and part7_optional_order_lc_etof_mapping
2. Creates a vocabulary DataFrame mapping columns from all sources to standard names (based on rate card columns)
3. Uses custom logic (if available) or LLM semantic matching to find column mappings
4. Filters to keep only relevant columns (rate card columns, SHIP_DATE, SHIPMENT_ID/delivery number/etof #/lc#)
"""

import pandas as pd
import os
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher

# Import processing functions
from part4_rate_card_processing import process_rate_card
from part1_etof_file_processing import process_etof_file
from part3_origin_file_processing import process_origin_file
from part7_optional_order_lc_etof_mapping import process_order_lc_etof_mapping

# Try to import lightweight ML libraries for semantic similarity
try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    SEMANTIC_AVAILABLE = True
except ImportError:
    SEMANTIC_AVAILABLE = False
    print("Note: sentence-transformers not available. Install with: pip install sentence-transformers scikit-learn")
    print("      Will use fuzzy string matching instead.")

# Initialize lightweight model for semantic similarity (if available)
_semantic_model = None
def get_semantic_model():
    """Get or initialize the semantic similarity model."""
    global _semantic_model
    if _semantic_model is None and SEMANTIC_AVAILABLE:
        try:
            _semantic_model = SentenceTransformer('all-MiniLM-L6-v2')  # ~80MB, fast
            print("   Loaded semantic similarity model for column mapping")
        except Exception as e:
            print(f"   Warning: Could not load semantic model: {e}")
            return None
    return _semantic_model


def calculate_string_similarity(str1, str2):
    """Calculate similarity between two strings (0-1)."""
    return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()


def normalize_for_semantics(text):
    """Normalize text by replacing semantic equivalents."""
    text = text.lower()
    text = text.replace('ship', 'origin')
    text = text.replace('cust', 'destination')
    text = text.replace('equipment type', 'cont_load')
    text = text.replace('equipmenttype', 'cont_load')
    text = text.replace('equipment', 'cont_load')
    # Postal code mappings
    text = text.replace('origin postal code', 'ship_post')
    text = text.replace('origin postal', 'ship_post')
    text = text.replace('destination postal code', 'cust_post')
    text = text.replace('destination postal', 'cust_post')
    text = text.replace('postal code', 'post')
    return text


def find_semantic_match_llm(target_col, candidate_cols, threshold=0.3):
    """Find the best semantic match for a column name using LLM."""
    if not candidate_cols:
        return None, 0.0
    
    target_lower = target_col.lower().strip()
    target_normalized = normalize_for_semantics(target_col)
    
    # Direct postal code mappings
    postal_mappings = {
        'origin postal code': 'ship_post',
        'origin postal': 'ship_post',
        'destination postal code': 'cust_post',
        'destination postal': 'cust_post',
        'ship_post': 'ship_post',
        'cust_post': 'cust_post'
    }
    
    # Check direct postal code mappings first
    target_for_postal = target_lower.replace(' ', '')
    for postal_key, postal_value in postal_mappings.items():
        if postal_key in target_lower or postal_key.replace(' ', '') in target_for_postal:
            for cand in candidate_cols:
                cand_lower = cand.lower().strip()
                if postal_value in cand_lower or cand_lower in postal_value:
                    return cand, 0.95
    
    # First try exact or very close matches
    for cand in candidate_cols:
        cand_lower = cand.lower().strip()
        cand_normalized = normalize_for_semantics(cand)
        
        if target_lower == cand_lower:
            return cand, 1.0
        
        if target_normalized == cand_normalized:
            return cand, 0.95
        
        if target_normalized in cand_normalized or cand_normalized in target_normalized:
            similarity = calculate_string_similarity(target_col, cand)
            if similarity > 0.7:
                return cand, similarity
    
    # Try semantic similarity if model is available
    model = get_semantic_model()
    if model is not None:
        try:
            enhanced_target = target_col.replace('SHIP', 'origin').replace('CUST', 'destination').replace('ship', 'origin').replace('cust', 'destination')
            enhanced_target = enhanced_target.replace('equipment type', 'cont_load').replace('equipmenttype', 'cont_load').replace('equipment', 'cont_load')
            enhanced_target = enhanced_target.replace('Origin postal code', 'SHIP_POST').replace('origin postal code', 'ship_post')
            enhanced_target = enhanced_target.replace('Destination postal code', 'CUST_POST').replace('destination postal code', 'cust_post')
            enhanced_target = enhanced_target.replace('SHIP_POST', 'ship_post').replace('CUST_POST', 'cust_post')
            
            enhanced_candidates = [c.replace('SHIP', 'origin').replace('CUST', 'destination').replace('ship', 'origin').replace('cust', 'destination')
                                 for c in candidate_cols]
            enhanced_candidates = [c.replace('equipment type', 'cont_load').replace('equipmenttype', 'cont_load').replace('equipment', 'cont_load')
                                 for c in enhanced_candidates]
            enhanced_candidates = [c.replace('Origin postal code', 'SHIP_POST').replace('origin postal code', 'ship_post')
                                 for c in enhanced_candidates]
            enhanced_candidates = [c.replace('Destination postal code', 'CUST_POST').replace('destination postal code', 'cust_post')
                                 for c in enhanced_candidates]
            enhanced_candidates = [c.replace('SHIP_POST', 'ship_post').replace('CUST_POST', 'cust_post')
                                 for c in enhanced_candidates]
            
            target_embedding = model.encode([enhanced_target])
            candidate_embeddings = model.encode(enhanced_candidates)
            similarities = cosine_similarity(target_embedding, candidate_embeddings)[0]
            
            best_idx = np.argmax(similarities)
            best_similarity = float(similarities[best_idx])
            
            if best_similarity >= threshold:
                return candidate_cols[best_idx], best_similarity
        except Exception as e:
            print(f"   Warning: Semantic matching failed: {e}, using fuzzy matching")
    
    # Fallback to fuzzy string matching
    best_match = None
    best_score = 0.0
    
    for cand in candidate_cols:
        similarity = calculate_string_similarity(target_col, cand)
        if similarity > best_score:
            best_score = similarity
            best_match = cand
    
    if best_score >= threshold:
        return best_match, best_score
    
    return None, best_score


def find_carrier_id_column(column_list):
    """Find the column that represents CARRIER ID."""
    carrier_keywords = ['carrier', 'carrier_id', 'carrier id']
    for col in column_list:
        col_lower = col.lower()
        for keyword in carrier_keywords:
            if keyword in col_lower:
                return col
    return None


def find_transport_mode_column(column_list):
    """Find the column that represents TRANSPORT MODE."""
    transport_keywords = ['transport', 'transport_mode', 'transport mode', 'mode']
    for col in column_list:
        col_lower = col.lower()
        for keyword in transport_keywords:
            if keyword in col_lower:
                return col
    return None



def check_custom_logic(carrier_id, shipper_id, transport_mode, custom_logic_dict):
    """
    Check if custom logic exists for the combination of carrier_id, shipper_id, transport_mode, and ship_port.
    
    Args:
        carrier_id: Carrier ID value
        shipper_id: Shipper ID value
        transport_mode: Transport mode value
        custom_logic_dict: Dictionary with keys as tuples (carrier_id, shipper_id, transport_mode, ship_port)
    
    Returns:
        Custom mapping if found, else None
    """
    if custom_logic_dict is None:
        return None
    
    # Try exact match
    key = (str(carrier_id), str(shipper_id), str(transport_mode))
    if key in custom_logic_dict:
        return custom_logic_dict[key]
    
    # Try partial matches (if some values are None/empty)
    for logic_key, logic_value in custom_logic_dict.items():
        match = True
        for i, val in enumerate(logic_key):
            if val and val != 'None' and val != '':
                if i == 0 and str(carrier_id) != val:
                    match = False
                    break
                elif i == 1 and str(shipper_id) != val:
                    match = False
                    break
                elif i == 2 and str(transport_mode) != val:
                    match = False
                    break
        if match:
            return logic_value
    
        return None


def is_date_column(column_name):
    """Check if column is related to SHIP_DATE."""
    date_keywords = ['date', 'ship_date', 'ship date', 'delivery_date', 'delivery date', 
                     'arrival_date', 'arrival date', 'invoice_date', 'invoice date']
    col_lower = column_name.lower()
    return any(keyword in col_lower for keyword in date_keywords)


def is_shipment_id_column(column_name):
    """Check if column is related to SHIPMENT_ID/delivery number/etof #/lc#."""
    shipment_keywords = ['shipment', 'shipment_id', 'shipment id', 'delivery', 'delivery number', 
                         'delivery_number', 'etof', 'etof #', 'etof#', 'lc', 'lc #', 'lc#', 
                         'order file', 'order_file', 'DELIVERY_NUMBER']
    col_lower = column_name.lower()
    return any(keyword in col_lower for keyword in shipment_keywords)


# CUSTOM LOGIC MAPPINGS
# Format: {(carrier_id, shipper_id, transport_mode, ship_port): {source_col: standard_col}}
# ship_port is the Origin Airport code (SHIP_PORT)
CUSTOM_LOGIC_MAPPINGS = {
    # Custom mapping for dairb: map LC column "SERVICE" to rate card column "Service"
    (None, 'dairb', None): {
        'SERVICE': 'Service'
    },
    # Example custom mappings - add your specific mappings here
    # ('CARRIER1', 'SHIPPER1', 'AIR', 'JFK'): {
    #     'Origin airport': 'Origin Airport Code',
    #     'Destination airport': 'Destination Airport Code'
    # },
    # Add more custom mappings as needed
}

# Columns to exclude from mapping
EXCLUDED_COLUMNS = [
    'ETOF #',
    'ETOF#',
    'LC #',
    'LC#',
    'Carrier',
    'Delivery Number',
    'DeliveryNumber',
    'Lane #'
]

# Rate card columns that should not be mapped (kept as-is)
RATE_CARD_EXCLUDED_COLUMNS = [
    'Valid to',
    'Valid from',
    'Valid To',
    'Valid From'
]


def is_excluded_column(column_name):
    """Check if a column name should be excluded (case-insensitive, handles variations)."""
    if not column_name:
        return False
    
    col_lower = str(column_name).lower().strip()
    
    # Check against excluded columns (case-insensitive)
    for excluded in EXCLUDED_COLUMNS:
        excluded_lower = str(excluded).lower().strip()
        # Exact match
        if col_lower == excluded_lower:
            return True
        # Check if column contains excluded keyword (for variations like "ETOF #" vs "ETOF#")
        if excluded_lower.replace(' ', '') in col_lower.replace(' ', '') or col_lower.replace(' ', '') in excluded_lower.replace(' ', ''):
            # Additional check: make sure it's not just a partial match
            if 'etof' in excluded_lower and 'etof' in col_lower:
                return True
            if 'lc' in excluded_lower and 'lc' in col_lower and '#' in col_lower:
                return True
            if excluded_lower == 'carrier' and col_lower == 'carrier':
                return True
            if 'delivery' in excluded_lower and 'delivery' in col_lower and 'number' in col_lower:
                return True
    
    return False


def create_vocabulary_dataframe(
    rate_card_file_path: str,
    etof_file_path: Optional[str] = None,
    origin_file_path: Optional[str] = None,
    order_files_path: Optional[str] = None,
    lc_input_path: Optional[str] = None,
    shipper_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Create a vocabulary DataFrame mapping columns from all sources to standard names.
    
    Args:
        rate_card_file_path: Path to rate card file
        etof_file_path: Optional path to ETOF file
        origin_file_path: Optional path to origin file
        order_files_path: Optional path to order files export
        lc_input_path: Optional path to LC input (file/folder/list)
        shipper_id: Optional shipper ID constant (used for custom logic matching)
    
    Returns:
        DataFrame with vocabulary mappings
        Columns: 
            - 'Source': Source of the column (ETOF, Origin, LC)
            - 'Source_Column': Original column name from source
            - 'Standard_Name': Standard column name (from rate card)
            - 'Mapping': Shows "Original_Column → Standard_Name" mapping
            - 'Mapping_Method': How it was mapped ('custom', 'LLM', 'fuzzy', 'keyword_match')
            - 'Confidence': Confidence score (0-1)
        Mapping_Method values: 'custom', 'LLM', 'fuzzy', 'keyword_match'
    """
    print("\n" + "="*80)
    print("CREATING VOCABULARY DATAFRAME")
    print("="*80)
    
    # Step 1: Get rate card columns (these are the standard names)
    print("\n1. Processing Rate Card...")
    try:
        rate_card_df, rate_card_columns, rate_card_conditions = process_rate_card(rate_card_file_path)
        print(f"   Found {len(rate_card_columns)} rate card columns")
        
        # Filter out excluded columns from rate card (case-insensitive)
        excluded_found = [col for col in rate_card_columns if is_excluded_column(col)]
        rate_card_columns = [col for col in rate_card_columns if not is_excluded_column(col)]
        
        if excluded_found:
            print(f"   Excluded {len(excluded_found)} columns from mapping: {excluded_found}")
            print(f"   Remaining rate card columns for mapping: {len(rate_card_columns)}")
    except Exception as e:
        print(f"   Error processing rate card: {e}")
        return pd.DataFrame()
    
    # Step 2: Collect columns from all sources (excluding specified columns)
    all_source_columns = {}
    
    if etof_file_path:
        print("\n2. Processing ETOF file...")
        try:
            etof_df, etof_columns = process_etof_file(etof_file_path)
            # Filter out excluded columns (case-insensitive)
            excluded_etof = [col for col in etof_columns if is_excluded_column(col)]
            etof_columns = [col for col in etof_columns if not is_excluded_column(col)]
            all_source_columns['ETOF'] = etof_columns
            print(f"   Found {len(etof_columns)} ETOF columns (excluded {len(excluded_etof)}: {excluded_etof})")
        except Exception as e:
            print(f"   Error processing ETOF: {e}")
    
    if origin_file_path:
        print("\n3. Processing Origin file...")
        try:
            # Try to detect if it's an EDI file (doesn't need header_row)
            file_ext = os.path.splitext(origin_file_path)[1].lower()
            if file_ext == '.edi':
                origin_df, origin_columns = process_origin_file(origin_file_path, header_row=None, end_column=None)
            else:
                # For CSV/Excel, try with header_row=1 as default
                origin_df, origin_columns = process_origin_file(origin_file_path, header_row=1, end_column=None)
            
            # Custom logic for shipper "dairb": rename "SHAI Reference" to "SHIPMENT_ID"
            if shipper_id and shipper_id.lower() == 'dairb' and origin_df is not None and not origin_df.empty:
                if 'SHAI Reference' in origin_df.columns:
                    origin_df = origin_df.rename(columns={'SHAI Reference': 'SHIPMENT_ID'})
                    origin_columns = origin_df.columns.tolist()
            
            # Filter out excluded columns (case-insensitive)
            excluded_origin = [col for col in origin_columns if is_excluded_column(col)]
            origin_columns = [col for col in origin_columns if not is_excluded_column(col)]
            all_source_columns['Origin'] = origin_columns
            print(f"   Found {len(origin_columns)} origin columns (excluded {len(excluded_origin)}: {excluded_origin})")
        except Exception as e:
            print(f"   Error processing origin file: {e}")
    
    if lc_input_path and etof_file_path:
        print("\n4. Processing LC/ETOF files...")
        try:
            # process_order_lc_etof_mapping now accepts optional order_files_path
            # If order_files_path is provided, uses order file mapping
            # If not provided, uses SHIPMENT_ID mapping
            lc_df, lc_columns = process_order_lc_etof_mapping(lc_input_path, etof_file_path, order_files_path=order_files_path)
            # Filter out excluded columns (case-insensitive)
            excluded_lc = [col for col in lc_columns if is_excluded_column(col)]
            lc_columns = [col for col in lc_columns if not is_excluded_column(col)]
            all_source_columns['LC'] = lc_columns
            print(f"   Found {len(lc_columns)} LC columns (excluded {len(excluded_lc)}: {excluded_lc})")
        except Exception as e:
            print(f"   Error processing LC files: {e}")
    
    # Step 3: Print all columns explored from each source
    print("\n" + "="*80)
    print("COLUMNS EXPLORED FROM EACH SOURCE")
    print("="*80)
    print(f"\nRate Card ({len(rate_card_columns)} columns):")
    for i, col in enumerate(rate_card_columns, 1):
        print(f"  {i}. {col}")
    
    for source_name, source_columns in all_source_columns.items():
        print(f"\n{source_name} ({len(source_columns)} columns):")
        for i, col in enumerate(source_columns, 1):
            print(f"  {i}. {col}")
    
    # Step 4: Find CARRIER_ID and TRANSPORT_MODE columns for custom logic
    carrier_id_col = None
    transport_mode_col = None
    
    # Try to find these columns in rate card first
    carrier_id_col = find_carrier_id_column(rate_card_columns)
    transport_mode_col = find_transport_mode_column(rate_card_columns)
    
    # Step 5: Create vocabulary mappings (one-to-one mapping)
    print("\n" + "="*80)
    print("CREATING VOCABULARY MAPPINGS (ONE-TO-ONE)")
    print("="*80)
    vocabulary_data = []
    
    # Track which source columns have been used (for one-to-one mapping)
    # Format: {source_name: set of used source columns}
    used_source_columns = {source_name: set() for source_name in all_source_columns.keys()}
    
    # Check if we have custom logic mappings
    has_custom_logic = len(CUSTOM_LOGIC_MAPPINGS) > 0
    if has_custom_logic:
        print(f"   Found {len(CUSTOM_LOGIC_MAPPINGS)} custom logic mapping(s)")
    
    # For each rate card column (standard name), find ONE match per source
    for standard_col in rate_card_columns:
        # Map to each source (one-to-one: one rate card column -> one source column per source)
        for source_name, source_columns in all_source_columns.items():
            # Skip if this rate card column already has a mapping for this source
            existing_mapping = [item for item in vocabulary_data 
                               if item['Standard_Name'] == standard_col and item['Source'] == source_name]
            if existing_mapping:
                continue  # Already mapped for this source
            
            # Get available source columns (not yet used)
            available_columns = [col for col in source_columns if col not in used_source_columns[source_name]]
            
            if not available_columns:
                continue  # No available columns in this source
            
            # Check custom logic first if available
            custom_mapping_found = False
            if has_custom_logic and shipper_id:
                # Check all custom logic entries for this standard column
                for (carrier_id_key, shipper_id_key, transport_mode_key), mapping_dict in CUSTOM_LOGIC_MAPPINGS.items():
                    # Check if shipper_id matches (if specified in custom logic)
                    if shipper_id_key and shipper_id_key != shipper_id:
                        continue
                    
                    # Check if this standard column has a custom mapping
                    if standard_col in mapping_dict.values():
                        # Find the source column that maps to this standard column
                        for source_col, mapped_standard in mapping_dict.items():
                            if mapped_standard == standard_col and source_col in available_columns:
                                # Double-check: skip if either column is excluded
                                if is_excluded_column(standard_col) or is_excluded_column(source_col):
                                    continue
                                vocabulary_data.append({
                                    'Standard_Name': standard_col,
                                    'Source': source_name,
                                    'Source_Column': source_col,
                                    'Mapping_Method': 'custom',
                                    'Confidence': 1.0
                                })
                                used_source_columns[source_name].add(source_col)
                                custom_mapping_found = True
                                break
                    
                    if custom_mapping_found:
                        break
                
                if custom_mapping_found:
                    continue
            
            # Use LLM/semantic matching if no custom mapping found
            match, confidence = find_semantic_match_llm(standard_col, available_columns, threshold=0.3)
            if match:
                # Double-check: skip if either column is excluded
                if is_excluded_column(standard_col) or is_excluded_column(match):
                    continue
                method = 'LLM' if SEMANTIC_AVAILABLE else 'fuzzy'
                vocabulary_data.append({
                    'Standard_Name': standard_col,
                    'Source': source_name,
                    'Source_Column': match,
                    'Mapping_Method': method,
                    'Confidence': confidence
                })
                used_source_columns[source_name].add(match)
    
    # Step 6: Create DataFrame and identify unmapped columns
    print("\nCreating vocabulary DataFrame...")
    
    # Create DataFrame
    df_vocabulary = pd.DataFrame(vocabulary_data)
    
    if not df_vocabulary.empty:
        # Add a mapping column that shows Original → Standard clearly
        df_vocabulary['Mapping'] = df_vocabulary['Source_Column'] + ' → ' + df_vocabulary['Standard_Name']
        
        # Reorder columns to make it clearer: show original name, then what it maps to
        column_order = ['Source', 'Source_Column', 'Standard_Name', 'Mapping', 'Mapping_Method', 'Confidence']
        df_vocabulary = df_vocabulary[column_order]
        
        # Sort by Source, then Standard_Name
        df_vocabulary = df_vocabulary.sort_values(['Source', 'Standard_Name'])
    
    # Step 7: Identify and print unmapped columns
    print("\n" + "="*80)
    print("UNMAPPED COLUMNS ANALYSIS")
    print("="*80)
    
    # Find unmapped rate card columns
    if not df_vocabulary.empty:
        mapped_rate_cols = set(df_vocabulary['Standard_Name'].unique())
    else:
        mapped_rate_cols = set()
    
    unmapped_rate_cols = set(rate_card_columns) - mapped_rate_cols
    
    print(f"\nRate Card Columns:")
    print(f"  Total: {len(rate_card_columns)}")
    print(f"  Mapped: {len(mapped_rate_cols)}")
    print(f"  Unmapped: {len(unmapped_rate_cols)}")
    if unmapped_rate_cols:
        print(f"\n  Unmapped Rate Card Columns:")
        for col in sorted(unmapped_rate_cols):
            print(f"    - {col}")
    
    # Find unmapped source columns (columns that could have matched but didn't due to one-to-one constraint)
    print(f"\nSource Files Columns:")
    for source_name, source_columns in all_source_columns.items():
        used_cols = used_source_columns.get(source_name, set())
        unmapped_source_cols = set(source_columns) - used_cols
        print(f"\n  {source_name}:")
        print(f"    Total: {len(source_columns)}")
        print(f"    Mapped: {len(used_cols)}")
        print(f"    Unmapped: {len(unmapped_source_cols)}")
        if unmapped_source_cols:
            print(f"    Unmapped {source_name} Columns:")
            for col in sorted(unmapped_source_cols):
                print(f"      - {col}")
    
    print(f"\n   Created vocabulary with {len(df_vocabulary)} mappings")
    print(f"   Rate card columns mapped: {len(mapped_rate_cols)} out of {len(rate_card_columns)}")
    if not df_vocabulary.empty:
        print(f"   Sources: {df_vocabulary['Source'].unique().tolist()}")
    
    # Show mapping method breakdown
    if not df_vocabulary.empty:
        method_counts = df_vocabulary['Mapping_Method'].value_counts()
        print(f"\n   Mapping methods:")
        for method, count in method_counts.items():
            print(f"     {method}: {count}")
    
    return df_vocabulary


def map_and_rename_columns(
    rate_card_file_path: str,
    etof_file_path: Optional[str] = None,
    origin_file_path: Optional[str] = None,
    origin_header_row: Optional[int] = None,
    origin_end_column: Optional[int] = None,
    order_files_path: Optional[str] = None,
    lc_input_path: Optional[str] = None,
    output_txt_path: str = "column_mapping_results.txt",
    ignore_rate_card_columns: Optional[List[str]] = None,
    shipper_id: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Map rate card columns to ETOF, LC, and Origin files, rename columns, and save results.
    
    Args:
        rate_card_file_path: Path to rate card file
        etof_file_path: Optional path to ETOF file
        origin_file_path: Optional path to origin file
        origin_header_row: Optional header row for origin file (required for CSV/Excel)
        origin_end_column: Optional end column for origin file
        order_files_path: Optional path to order files export
        lc_input_path: Optional path to LC input (file/folder/list)
        output_txt_path: Path to save the mapping results text file
        ignore_rate_card_columns: Optional list of rate card column names to ignore/delete from processing
        shipper_id: Optional shipper ID for custom logic (e.g., "dairb")
    
    Returns:
        Tuple: (etof_dataframe_renamed, lc_dataframe_renamed, origin_dataframe_renamed)
    """
    # Step 1: Get rate card columns
    try:
        rate_card_df, rate_card_columns_all, rate_card_conditions = process_rate_card(rate_card_file_path)
        
        # Filter out ignored columns
        if ignore_rate_card_columns is None:
            ignore_rate_card_columns = []
        
        # Remove ignored columns from rate card dataframe
        if ignore_rate_card_columns:
            columns_to_drop = [col for col in ignore_rate_card_columns if col in rate_card_df.columns]
            if columns_to_drop:
                rate_card_df = rate_card_df.drop(columns=columns_to_drop)
        
        # Update rate_card_columns_all to exclude ignored columns
        rate_card_columns_all = [col for col in rate_card_columns_all if col not in ignore_rate_card_columns]
        
        rate_card_columns_to_map = [
            col for col in rate_card_columns_all 
            if not is_excluded_column(col) and col not in RATE_CARD_EXCLUDED_COLUMNS
        ]
        rate_card_columns = rate_card_columns_to_map
    except Exception:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Step 2: Get ETOF, LC, and Origin dataframes
    etof_df = None
    lc_df = None
    origin_df = None
    
    if etof_file_path:
        try:
            etof_df, etof_columns = process_etof_file(etof_file_path)
        except Exception:
            pass
    
    if origin_file_path:
        try:
            file_ext = os.path.splitext(origin_file_path)[1].lower()
            if file_ext == '.edi':
                origin_df, origin_columns = process_origin_file(origin_file_path, header_row=None, end_column=origin_end_column)
            else:
                if origin_header_row is None:
                    origin_header_row = 1
                origin_df, origin_columns = process_origin_file(origin_file_path, header_row=origin_header_row, end_column=origin_end_column)
            
            # Custom logic for shipper "dairb": rename "SHAI Reference" to "SHIPMENT_ID"
            if shipper_id and shipper_id.lower() == 'dairb' and origin_df is not None and not origin_df.empty:
                if 'SHAI Reference' in origin_df.columns:
                    origin_df = origin_df.rename(columns={'SHAI Reference': 'SHIPMENT_ID'})
                    origin_columns = origin_df.columns.tolist()
        except Exception:
            pass
    
    if lc_input_path and etof_file_path:
        try:
            # process_order_lc_etof_mapping now accepts optional order_files_path
            # If order_files_path is provided, uses order file mapping
            # If not provided, uses SHIPMENT_ID mapping
            lc_df, lc_columns = process_order_lc_etof_mapping(lc_input_path, etof_file_path, order_files_path=order_files_path)
        except Exception:
            pass
    
    # Step 3: Find mappings for each rate card column
    if etof_df is None and lc_df is None and origin_df is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Columns to always keep (even if not in rate card)
    keep_columns = ['ETOF #', 'ETOF#', 'LC #', 'LC#', 'Carrier', 'Delivery Number', 'DeliveryNumber', 
                   'Shipment ID', 'ShipmentID', 'shipment id', 'shipmentid', 'SHIPMENT_ID']
    
    # Mapping results
    mapping_results = []
    etof_mappings = {}  # {rate_card_col: etof_col}
    lc_mappings = {}    # {rate_card_col: lc_col}
    origin_mappings = {}  # {rate_card_col: origin_col}
    
    # Track which source columns have been used (cannot be reused)
    used_etof_columns = set()
    used_lc_columns = set()
    used_origin_columns = set()
    
    for rate_card_col in rate_card_columns:
        etof_match = None
        lc_match = None
        origin_match = None
        
        # Find match in ETOF (only if column hasn't been used yet)
        if etof_df is not None and not etof_df.empty:
            etof_columns = [col for col in etof_df.columns 
                          if not is_excluded_column(col) and col not in used_etof_columns]
            if etof_columns:
                match, confidence = find_semantic_match_llm(rate_card_col, etof_columns, threshold=0.3)
                if match and not is_excluded_column(match) and match not in used_etof_columns:
                    etof_match = match
                    etof_mappings[rate_card_col] = match
                    used_etof_columns.add(match)
        
        # Find match in LC (only if column hasn't been used yet)
        if lc_df is not None and not lc_df.empty:
            lc_columns = [col for col in lc_df.columns 
                        if not is_excluded_column(col) and col not in used_lc_columns]
            if lc_columns:
                # Check custom logic first if available
                custom_match_found = False
                if shipper_id and len(CUSTOM_LOGIC_MAPPINGS) > 0:
                    for (carrier_id_key, shipper_id_key, transport_mode_key), mapping_dict in CUSTOM_LOGIC_MAPPINGS.items():
                        # Check if shipper_id matches
                        if shipper_id_key and shipper_id_key == shipper_id:
                            # Check if this rate card column has a custom mapping
                            if rate_card_col in mapping_dict.values():
                                # Find the source column that maps to this rate card column
                                for source_col, mapped_standard in mapping_dict.items():
                                    if mapped_standard == rate_card_col and source_col in lc_columns:
                                        if not is_excluded_column(source_col) and source_col not in used_lc_columns:
                                            lc_match = source_col
                                            lc_mappings[rate_card_col] = source_col
                                            used_lc_columns.add(source_col)
                                            custom_match_found = True
                                            break
                        if custom_match_found:
                            break
                
                # Use semantic matching if no custom mapping found
                if not custom_match_found:
                    match, confidence = find_semantic_match_llm(rate_card_col, lc_columns, threshold=0.3)
                    if match and not is_excluded_column(match) and match not in used_lc_columns:
                        lc_match = match
                        lc_mappings[rate_card_col] = match
                        used_lc_columns.add(match)
        
        # Find match in Origin (only if column hasn't been used yet)
        if origin_df is not None and not origin_df.empty:
            origin_columns = [col for col in origin_df.columns 
                            if not is_excluded_column(col) and col not in used_origin_columns]
            if origin_columns:
                match, confidence = find_semantic_match_llm(rate_card_col, origin_columns, threshold=0.3)
                if match and not is_excluded_column(match) and match not in used_origin_columns:
                    origin_match = match
                    origin_mappings[rate_card_col] = match
                    used_origin_columns.add(match)
        
        mapping_results.append({
            'Rate_Card_Column': rate_card_col,
            'ETOF_Column': etof_match if etof_match else 'NONE',
            'LC_Column': lc_match if lc_match else 'NONE',
            'Origin_Column': origin_match if origin_match else 'NONE'
        })
    
    # Step 4: Rename columns and include ALL rate card columns
    all_rate_card_cols_for_output = rate_card_columns_all.copy()
    
    etof_df_renamed = None
    lc_df_renamed = None
    origin_df_renamed = None
    
    def create_output_dataframe(source_df, source_mappings, source_name, keep_cols_list, specific_keep_list, all_rate_card_cols):
        """Helper function to create output dataframe with rate card columns and key columns only."""
        if source_df is None or source_df.empty:
            return None

        output_df = source_df.copy()
        rename_dict = {}
        columns_to_keep = []
        
        # Step 1: Add rate card mapped columns (will be renamed to "RateCardColumn (OriginalColumn)")
        for rate_card_col, source_col in source_mappings.items():
            if source_col in output_df.columns:
                rename_dict[source_col] = f"{rate_card_col} ({source_col})"
                columns_to_keep.append(source_col)
        
        # Step 2: Add columns to always keep (ETOF #, LC #, Carrier, Delivery Number)
        for keep_col in keep_cols_list:
            # Try to find the column (case-insensitive and handle variations)
            found = False
            for col in output_df.columns:
                col_normalized = col.lower().replace(' ', '').replace('#', '#')
                keep_normalized = keep_col.lower().replace(' ', '').replace('#', '#')
                if col_normalized == keep_normalized:
                    if col not in columns_to_keep:
                        columns_to_keep.append(col)
                    found = True
                    break
            if not found:
                # Also check if the column name itself matches (exact match)
                if keep_col in output_df.columns and keep_col not in columns_to_keep:
                    columns_to_keep.append(keep_col)
        
        # Step 3: Add source-specific columns to keep (Loading date for ETOF, SHIP_DATE for LC)
        for keep_col in specific_keep_list:
            # Try to find the column (case-insensitive)
            for col in output_df.columns:
                if col.lower() == keep_col.lower():
                    if col not in columns_to_keep:
                        columns_to_keep.append(col)
                    break
        
        # Step 4: Rename columns first (before filtering)
        output_df.rename(columns=rename_dict, inplace=True)
        
        # Step 5: Now rename "RateCardColumn (OriginalColumn)" to just "RateCardColumn"
        rename_to_standard = {}
        for col in output_df.columns:
            if ' (' in col and col.endswith(')'):
                standard_name = col.split(' (')[0]
                # Only rename if it's a rate card column
                if standard_name in all_rate_card_cols:
                    rename_to_standard[col] = standard_name
        
        if rename_to_standard:
            output_df.rename(columns=rename_to_standard, inplace=True)
            # Update columns_to_keep list with renamed columns
            updated_columns_to_keep = []
            for col in columns_to_keep:
                if col in rename_to_standard:
                    updated_columns_to_keep.append(rename_to_standard[col])
                elif col in output_df.columns:
                    updated_columns_to_keep.append(col)
            columns_to_keep = updated_columns_to_keep
        
        # Step 6: Add ALL rate card columns that are not yet in the dataframe (as empty columns)
        # Only add columns that don't have a mapping (were not mapped from this source)
        for rate_card_col in all_rate_card_cols:
            # Skip if this column was excluded from mapping
            if is_excluded_column(rate_card_col) or rate_card_col in RATE_CARD_EXCLUDED_COLUMNS:
                continue
            
            # Check if this column is already in the dataframe (was mapped)
            if rate_card_col not in output_df.columns:
                # Check if this rate card column has a mapping from this source
                # If it does, we should have already added it, so skip
                # If it doesn't, add it as empty
                if rate_card_col not in source_mappings:
                    # No mapping found - add as empty column
                    output_df[rate_card_col] = None
                    if rate_card_col not in columns_to_keep:
                        columns_to_keep.append(rate_card_col)
        
        # Step 7: Build final column list - ONLY rate card columns + key columns (LC #, ETOF #, Carrier, Loading date/SHIP_DATE)
        final_columns = []
        
        # Add all rate card columns first (mapped or unmapped)
        for rate_card_col in all_rate_card_cols:
            # Skip excluded columns
            if is_excluded_column(rate_card_col) or rate_card_col in RATE_CARD_EXCLUDED_COLUMNS:
                continue
            
            # Add rate card column if it exists
            if rate_card_col in output_df.columns:
                final_columns.append(rate_card_col)
        
        # Add key columns: LC #, ETOF #, Carrier, Delivery Number, Shipment ID
        key_columns_to_find = ['ETOF #', 'ETOF#', 'LC #', 'LC#', 'Carrier', 'Delivery Number', 'DeliveryNumber', 
                              'Shipment ID', 'ShipmentID', 'shipment id', 'shipmentid', 'SHIPMENT_ID']
        for key_col in key_columns_to_find:
            # Find the column (case-insensitive, handle variations)
            for col in output_df.columns:
                col_normalized = col.lower().replace(' ', '').replace('#', '#')
                key_normalized = key_col.lower().replace(' ', '').replace('#', '#')
                if col_normalized == key_normalized:
                    if col not in final_columns:
                        final_columns.append(col)
                    break
        
        # Add source-specific columns: Loading date (ETOF) or SHIP_DATE (LC)
        for specific_col in specific_keep_list:
            # Find the column (case-insensitive)
            for col in output_df.columns:
                if col.lower() == specific_col.lower():
                    if col not in final_columns:
                        final_columns.append(col)
                    break
        
        # Step 8: Filter to keep ONLY the final columns
        output_df = output_df[final_columns]
        
        # Step 9: Ensure Carrier column exists (add if not present)
        carrier_col_found = False
        carrier_variations = ['Carrier', 'carrier', 'CARRIER']
        
        for col in output_df.columns:
            if str(col).strip() in carrier_variations:
                carrier_col_found = True
                break
        
        if not carrier_col_found:
            output_df['Carrier'] = None
            final_columns.append('Carrier')
        
        delivery_col_found = False
        delivery_variations = ['Delivery Number', 'DeliveryNumber', 'delivery number', 'deliverynumber', 
                              'Delivery', 'delivery', 'DELIVERY', 'DELIVERY_NUMBER']
        
        for col in output_df.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '').replace('_', '')
            for variation in delivery_variations:
                var_lower = variation.lower().replace(' ', '').replace('_', '')
                if col_lower == var_lower or ('delivery' in col_lower and 'number' in col_lower):
                    delivery_col_found = True
                    break
            if delivery_col_found:
                break
        
        if not delivery_col_found:
            output_df['Delivery Number'] = None
            final_columns.append('Delivery Number')
        
        shipment_id_col_found = False
        shipment_id_variations = ['Shipment ID', 'ShipmentID', 'shipment id', 'shipmentid', 
                                 'SHIPMENT_ID', 'SHIPMENT ID', 'Shipment', 'shipment', 'SHIPMENT']
        
        for col in output_df.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '').replace('_', '')
            for variation in shipment_id_variations:
                var_lower = variation.lower().replace(' ', '').replace('_', '')
                if col_lower == var_lower or ('shipment' in col_lower and 'id' in col_lower):
                    shipment_id_col_found = True
                    break
            if shipment_id_col_found:
                break
        
        if not shipment_id_col_found:
            output_df['Shipment ID'] = None
            final_columns.append('Shipment ID')
        
        return output_df
    
    # Process ETOF
    if etof_df is not None:
        etof_specific_keep = ['Loading date', 'Loading Date', 'loading date', 'LOADING DATE']
        etof_df_renamed = create_output_dataframe(
            etof_df, etof_mappings, 'ETOF', keep_columns, etof_specific_keep, all_rate_card_cols_for_output
        )
    
    # Process LC
    if lc_df is not None:
        lc_specific_keep = ['SHIP_DATE', 'ship_date', 'Ship Date', 'ship date', 'SHIP DATE']
        lc_df_renamed = create_output_dataframe(
            lc_df, lc_mappings, 'LC', keep_columns, lc_specific_keep, all_rate_card_cols_for_output
        )
        
        # Print LC column list
        if lc_df_renamed is not None and not lc_df_renamed.empty:
            print(f"\n   LC DataFrame Columns ({len(lc_df_renamed.columns)}):")
            for i, col in enumerate(lc_df_renamed.columns, 1):
                print(f"     {i}. {col}")
    
    # Process Origin
    if origin_df is not None:
        origin_specific_keep = []  # No specific columns for origin
        origin_df_renamed = create_output_dataframe(
            origin_df, origin_mappings, 'Origin', keep_columns, origin_specific_keep, all_rate_card_cols_for_output
        )
    
    # Step 4.5: Fill LC Carrier column from ETOF Carrier ID
    if lc_df_renamed is not None and etof_df_renamed is not None:
        print("\n4.5. Filling LC Carrier column from ETOF Carrier ID...")
        
        # Find ETOF # column in both dataframes
        lc_etof_col = None
        etof_etof_col = None
        
        # Find ETOF # in LC dataframe
        etof_patterns = ['ETOF #', 'ETOF#', 'etof #', 'etof#']
        for col in lc_df_renamed.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '')
            for pattern in etof_patterns:
                if col_lower == pattern.lower().replace(' ', '') or col_str == pattern:
                    lc_etof_col = col
                    break
            if lc_etof_col:
                break
        
        # Find ETOF # in ETOF dataframe
        for col in etof_df_renamed.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '')
            for pattern in etof_patterns:
                if col_lower == pattern.lower().replace(' ', '') or col_str == pattern:
                    etof_etof_col = col
                    break
            if etof_etof_col:
                break
        
        # Find Carrier ID in ETOF dataframe
        etof_carrier_col = None
        carrier_patterns = ['Carrier', 'carrier', 'CARRIER', 'Carrier ID', 'CarrierID', 'carrier id', 'carrierid']
        for col in etof_df_renamed.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '')
            for pattern in carrier_patterns:
                if col_lower == pattern.lower().replace(' ', '') or col_str == pattern:
                    etof_carrier_col = col
                    break
            if etof_carrier_col:
                break
        
        # Find Carrier column in LC dataframe
        lc_carrier_col = None
        for col in lc_df_renamed.columns:
            col_str = str(col).strip()
            if col_str.lower() == 'carrier':
                lc_carrier_col = col
                break
        
        if lc_etof_col and etof_etof_col and etof_carrier_col and lc_carrier_col:
            etof_mapping = {}
            for idx, row in etof_df_renamed.iterrows():
                etof_num = row.get(etof_etof_col)
                carrier_id = row.get(etof_carrier_col)
                
                if pd.notna(etof_num) and pd.notna(carrier_id):
                    etof_num_str = str(etof_num).strip()
                    carrier_id_str = str(carrier_id).strip()
                    if etof_num_str and carrier_id_str:
                        etof_mapping[etof_num_str] = carrier_id_str
            
            for idx, row in lc_df_renamed.iterrows():
                lc_etof_num = row.get(lc_etof_col)
                
                if pd.notna(lc_etof_num):
                    lc_etof_num_str = str(lc_etof_num).strip()
                    if lc_etof_num_str in etof_mapping:
                        lc_df_renamed.at[idx, lc_carrier_col] = etof_mapping[lc_etof_num_str]
            
            # Show statistics
            total_lc_rows = len(lc_df_renamed)
            lc_rows_with_etof = len(lc_df_renamed[lc_df_renamed[lc_etof_col].notna()])
            lc_rows_with_carrier = len(lc_df_renamed[lc_df_renamed[lc_carrier_col].notna()])
            
            print(f"   LC statistics:")
            print(f"     Total rows: {total_lc_rows}")
            print(f"     Rows with ETOF #: {lc_rows_with_etof}")
            print(f"     Rows with Carrier (after fill): {lc_rows_with_carrier}")
        else:
            missing_cols = []
            if not lc_etof_col:
                missing_cols.append("LC ETOF #")
            if not etof_etof_col:
                missing_cols.append("ETOF ETOF #")
            if not etof_carrier_col:
                missing_cols.append("ETOF Carrier ID")
            if not lc_carrier_col:
                missing_cols.append("LC Carrier")
            
    
    # Step 6: Update ETOF dataframe with values from Origin dataframe
    # Skip if LC file was provided, only update if ETOF and Origin files were provided
    if lc_df_renamed is None and etof_df_renamed is not None and origin_df_renamed is not None:
        try:
            # Find matching columns
            shipment_id_col_etof = None
            shipment_id_col_origin = None
            delivery_col_etof = None
            delivery_col_origin = None
            
            # Find SHIPMENT_ID columns
            shipment_variations = ['SHIPMENT_ID', 'Shipment ID', 'ShipmentID', 'shipment id', 'shipmentid']
            for col in etof_df_renamed.columns:
                if str(col).strip() in shipment_variations or str(col).strip().upper() == 'SHIPMENT_ID':
                    shipment_id_col_etof = col
                    break
            for col in origin_df_renamed.columns:
                if str(col).strip() in shipment_variations or str(col).strip().upper() == 'SHIPMENT_ID':
                    shipment_id_col_origin = col
                    break
            
            # Find Delivery Number columns
            delivery_variations = ['Delivery Number', 'DeliveryNumber', 'delivery number', 'deliverynumber', 'DELIVERY_NUMBER']
            for col in etof_df_renamed.columns:
                col_str = str(col).strip()
                if col_str in delivery_variations or 'delivery' in col_str.lower() and 'number' in col_str.lower():
                    delivery_col_etof = col
                    break
            for col in origin_df_renamed.columns:
                col_str = str(col).strip()
                if col_str in delivery_variations or 'delivery' in col_str.lower() and 'number' in col_str.lower():
                    delivery_col_origin = col
                    break
            
            # Create mapping from Origin dataframe: (shipment_id, delivery_num) -> row data
            origin_dict_by_shipment = {}
            origin_dict_by_delivery = {}
            
            for idx, row in origin_df_renamed.iterrows():
                shipment_id = str(row.get(shipment_id_col_origin, '')).strip() if shipment_id_col_origin and pd.notna(row.get(shipment_id_col_origin)) else None
                delivery_num = str(row.get(delivery_col_origin, '')).strip() if delivery_col_origin and pd.notna(row.get(delivery_col_origin)) else None
                
                if shipment_id and shipment_id.lower() != 'nan':
                    origin_dict_by_shipment[shipment_id] = {
                        'delivery': delivery_num if delivery_num and delivery_num.lower() != 'nan' else None,
                        'row': row.to_dict()
                    }
                
                if delivery_num and delivery_num.lower() != 'nan':
                    origin_dict_by_delivery[delivery_num] = row.to_dict()
            
            # Update ETOF dataframe
            common_cols = [col for col in etof_df_renamed.columns if col in origin_df_renamed.columns 
                          and col not in ['ETOF #', 'ETOF#', 'LC #', 'LC#', 'Carrier', 'Loading date', 'Loading Date']]
            
            updated_count = 0
            for idx, row in etof_df_renamed.iterrows():
                shipment_id = str(row.get(shipment_id_col_etof, '')).strip() if shipment_id_col_etof and pd.notna(row.get(shipment_id_col_etof)) else None
                delivery_num = str(row.get(delivery_col_etof, '')).strip() if delivery_col_etof and pd.notna(row.get(delivery_col_etof)) else None
                
                origin_row = None
                
                # First try: SHIPMENT_ID matching
                # If SHIPMENT_ID matches, also verify DELIVERY_NUMBER matches (if both have it)
                if shipment_id and shipment_id.lower() != 'nan' and shipment_id in origin_dict_by_shipment:
                    origin_data = origin_dict_by_shipment[shipment_id]
                    # If both ETOF and Origin have delivery number, they must match
                    if delivery_num and delivery_num.lower() != 'nan' and origin_data['delivery']:
                        if origin_data['delivery'] == delivery_num:
                            origin_row = origin_data['row']
                    else:
                        # SHIPMENT_ID matches, and either no delivery number in ETOF or no delivery number in Origin - use it
                        origin_row = origin_data['row']
                
                # Fallback: If no SHIPMENT_ID in ETOF or no match, use DELIVERY_NUMBER
                if origin_row is None:
                    if not shipment_id or shipment_id.lower() == 'nan':
                        # No SHIPMENT_ID in ETOF - use DELIVERY_NUMBER
                        if delivery_num and delivery_num.lower() != 'nan' and delivery_num in origin_dict_by_delivery:
                            origin_row = origin_dict_by_delivery[delivery_num]
                    # If SHIPMENT_ID didn't match, also try DELIVERY_NUMBER as fallback
                    elif delivery_num and delivery_num.lower() != 'nan' and delivery_num in origin_dict_by_delivery:
                        origin_row = origin_dict_by_delivery[delivery_num]
                
                # Update NaN columns with values from Origin
                if origin_row:
                    for col in common_cols:
                        if pd.isna(etof_df_renamed.at[idx, col]) or etof_df_renamed.at[idx, col] is None:
                            origin_value = origin_row.get(col)
                            if pd.notna(origin_value) and origin_value is not None:
                                etof_df_renamed.at[idx, col] = origin_value
                                updated_count += 1
        except Exception:
            pass
    
    # Step 7: Save mapping to txt file
    from pathlib import Path
    output_folder = Path(__file__).parent / "partly_df"
    output_folder.mkdir(exist_ok=True)
    txt_output_path = output_folder / output_txt_path
    
    with open(txt_output_path, 'w', encoding='utf-8') as f:
        f.write("COLUMN MAPPING RESULTS\n")
        f.write("="*80 + "\n\n")
        f.write("MAPPINGS: Rate Card Column -> ETOF Column / LC Column / Origin Column\n")
        f.write("="*80 + "\n\n")
        for result in mapping_results:
            f.write(f"{result['Rate_Card_Column']} -> ETOF: {result['ETOF_Column']}, LC: {result['LC_Column']}, Origin: {result['Origin_Column']}\n")
        f.write("\n" + "="*80 + "\n")
        f.write("DETAILED MAPPINGS\n")
        f.write("="*80 + "\n\n")
        f.write("ETOF Mappings:\n")
        for rate_card_col, etof_col in etof_mappings.items():
            f.write(f"  {rate_card_col} <- {etof_col}\n")
        f.write("\nLC Mappings:\n")
        for rate_card_col, lc_col in lc_mappings.items():
            f.write(f"  {rate_card_col} <- {lc_col}\n")
        f.write("\nOrigin Mappings:\n")
        for rate_card_col, origin_col in origin_mappings.items():
            f.write(f"  {rate_card_col} <- {origin_col}\n")
    
    # Step 8: Save dataframes to Excel file
    excel_output_path = output_folder / "vocabulary_mapping.xlsx"
    with pd.ExcelWriter(excel_output_path, engine='openpyxl') as writer:
        if etof_df_renamed is not None and not etof_df_renamed.empty:
            etof_df_renamed.to_excel(writer, sheet_name='ETOF', index=False)
        if lc_df_renamed is not None and not lc_df_renamed.empty:
            lc_df_renamed.to_excel(writer, sheet_name='LC', index=False)
        if origin_df_renamed is not None and not origin_df_renamed.empty:
            origin_df_renamed.to_excel(writer, sheet_name='Origin', index=False)
        
        # Save mapping DataFrame
        mapping_df = pd.DataFrame(mapping_results)
        if not mapping_df.empty:
            mapping_df.to_excel(writer, sheet_name='Mapping', index=False)
    
    return etof_df_renamed, lc_df_renamed, origin_df_renamed


# Example usage
#if __name__ == "__main__":
#    try:
        # Main function: Map and rename columns
#        etof_renamed, lc_renamed, origin_renamed = map_and_rename_columns(
#            rate_card_file_path="rate_dairb.xlsx",
 #           etof_file_path="etofs_dairb.xlsx",
            #origin_file_path="file_dairb.xlsx",
            #origin_header_row=16,
            #origin_end_column=33,
            #order_files_path="Order_files_export.xls.xlsx",
#            lc_input_path="LC_Bollore ES (EUR)_ADSESPR03Bollore_ADS_Airfreight_ES_202509_CDP_I250014731_.xml",
#            output_txt_path="column_mapping_results.txt",
 #           ignore_rate_card_columns=["Business Unit Name", "Remark"],
 #           shipper_id="dairb"  # Custom logic: maps "SHAI Reference" to "SHIPMENT_ID" for dairb
  #      )
        
 #   except Exception:
  #      pass
#

