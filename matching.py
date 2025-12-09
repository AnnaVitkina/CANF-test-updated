"""
Compare and Find (CANF) - Match Shipments with Rate Card

This script:
1. Gets rate card from part4_rate_card_processing.py
2. Gets ETOF and LC dataframes from vocabular.py output (partly_df/vocabulary_mapping.xlsx)
3. Uses LC dataframe if present, otherwise uses ETOF dataframe
4. Matches shipments with Rate Card entries and identifies discrepancies
"""

import pandas as pd
import os
import re

def normalize_value(value):
    """Converts a value to lowercase string, removes spaces and underscores, and handles NaN."""
    if pd.isna(value):
        return None

    # Attempt to convert to a number if it looks like one, then convert to int if possible
    try:
        # Convert to string first to handle cases like numbers stored as strings (e.g., '7719')
        # and then to float for numeric conversion
        num_val = float(str(value))
        if num_val == int(num_val):  # Check if it's an integer number (e.g., 7719.0)
            value = int(num_val)
        else:  # Keep as float if it has decimal (e.g., 123.45)
            value = num_val
    except (ValueError, TypeError):
        # Not a number, keep original value (which will be string or other type)
        pass

    # Convert to string and apply lowercasing and cleaning
    return str(value).lower().replace(" ", "").replace("_", "")


def normalize_column_name(col_name):
    """Normalize column names for comparison (lowercase, remove spaces/underscores)."""
    if col_name is None:
        return None
    return str(col_name).lower().replace(" ", "").replace("_", "")


# Note: extract_country_code is already applied in part1_etof_file_processing.py
# No need to duplicate it here as vocabular.py uses processed dataframes


def load_conditions():
    """Load conditional rules from Filtered_Rate_Card_with_Conditions.xlsx."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    conditions_file = os.path.join(script_dir, "Filtered_Rate_Card_with_Conditions.xlsx")
    
    if not os.path.exists(conditions_file):
        print(f"Warning: {conditions_file} not found. Conditions will not be validated.")
        return {}
    
    try:
        df_conditions = pd.read_excel(conditions_file, sheet_name='Conditions')
        
        # Parse conditions into a dictionary: {column_name: [list of condition rules]}
        conditions_dict = {}
        current_column = None
        
        for _, row in df_conditions.iterrows():
            column = row.get('Column', '')
            condition_rule = row.get('Condition Rule', '')
            
            if pd.notna(column) and str(column).strip() and str(column).strip() != 'nan':
                current_column = str(column).strip()
                if current_column not in conditions_dict:
                    conditions_dict[current_column] = []
            
            if pd.notna(condition_rule) and str(condition_rule).strip() and current_column:
                condition_text = str(condition_rule).strip()
                # Skip header lines like "Conditional rules:"
                if condition_text.lower() not in ['conditional rules:', 'conditional rules']:
                    conditions_dict[current_column].append(condition_text)
        
        print(f"\nLoaded conditions for {len(conditions_dict)} columns")
        return conditions_dict
    except Exception as e:
        print(f"Warning: Could not load conditions: {e}")
        return {}


def parse_condition(condition_text, rate_card_value):
    """Parse a condition rule and extract the value it applies to.
    
    Example: "NAC: RATE_TYPE is empty in any item and does not contain FAK in any item"
    Returns: ('NAC', condition_logic)
    """
    if not condition_text or pd.isna(condition_text):
        return None, None
    
    condition_text = str(condition_text).strip()
    
    # Check if condition starts with a value followed by colon (e.g., "NAC: ...")
    if ':' in condition_text:
        parts = condition_text.split(':', 1)
        condition_value = parts[0].strip()
        condition_logic = parts[1].strip() if len(parts) > 1 else ''
        
        return condition_value, condition_logic
    
    return None, condition_text


def value_satisfies_condition(resmed_value, rate_card_value, condition_text):
    """Check if a ResMed value satisfies the condition for a given rate card value.
    
    Args:
        resmed_value: The value from ResMed dataframe
        rate_card_value: The value from Rate Card (e.g., 'NAC')
        condition_text: The condition rule text
    
    Returns:
        True if the value satisfies the condition, False otherwise
    
    Example:
        condition_text = "NAC: RATE_TYPE is empty in any item and does not contain FAK in any item"
        rate_card_value = "NAC"
        resmed_value = nan (empty)
        Returns: True (because empty satisfies "is empty")
    """
    if not condition_text or pd.isna(condition_text):
        return False
    
    condition_text = str(condition_text).strip()
    condition_lower = condition_text.lower()
    rate_card_val_str = str(rate_card_value).lower() if pd.notna(rate_card_value) else ''
    
    # Check if condition is for this rate card value (format: "1. NAC: ..." or "NAC: ...")
    if ':' in condition_text:
        # Handle numbered conditions like "1. NAC:" or "1.NAC:" or just "NAC:"
        # Remove leading number and dot if present (e.g., "1. " or "1.")
        condition_text_cleaned = re.sub(r'^\d+\.\s*', '', condition_text)
        condition_parts = condition_text_cleaned.split(':', 1)
        condition_value = condition_parts[0].strip()
        condition_logic = condition_parts[1].strip() if len(condition_parts) > 1 else ''
        
        # Check if this condition applies to the rate card value
        if rate_card_val_str and condition_value.lower() != rate_card_val_str:
            return False
        
        condition_text = condition_logic  # Use only the logic part
        condition_lower = condition_text.lower()
    
    # Check if ResMed value is empty/NaN
    is_empty = pd.isna(resmed_value) or str(resmed_value).strip() == '' or str(resmed_value).lower() in ['nan', 'none', 'null', '']
    resmed_val_str = str(resmed_value).lower() if pd.notna(resmed_value) else ''
    
    # Parse condition logic
    # Example: "RATE_TYPE is empty in any item and does not contain FAK in any item"
    
    # Check "is empty" condition
    if 'is empty' in condition_lower or 'is empty in any item' in condition_lower:
        if is_empty:
            # Value is empty - check if there are additional conditions
            # If condition has "and does not contain", empty values satisfy this (empty doesn't contain anything)
            if 'does not contain' in condition_lower or 'and' in condition_lower:
                # For "and" conditions, all must be satisfied
                # Empty value satisfies "is empty" and "does not contain X" (empty doesn't contain anything)
                return True
            return True
    
    # Check "does not contain" condition
    if 'does not contain' in condition_lower:
        if is_empty:
            return True  # Empty values don't contain anything
        
        # Extract what it should not contain
        parts = condition_lower.split('does not contain')
        if len(parts) > 1:
            forbidden_part = parts[1].split('in any item')[0].strip()
            # Handle comma-separated values (e.g., "EY,ETIHAD,ETIHAD AIRWAYS")
            forbidden_values = [v.strip() for v in forbidden_part.split(',')]
            # Check if ResMed value contains any forbidden value
            for forbidden in forbidden_values:
                if forbidden and forbidden in resmed_val_str:
                    return False  # Contains forbidden value - condition not satisfied
            return True  # Doesn't contain any forbidden value
    
    # Check "does not equal" condition
    if 'does not equal' in condition_lower or 'does not equal to' in condition_lower:
        if is_empty:
            return True  # Empty values don't equal anything
        
        parts = condition_lower.split('does not equal')
        if len(parts) > 1:
            forbidden_part = parts[1].split('in any item')[0].strip()
            # Handle comma-separated values
            forbidden_values = [v.strip() for v in forbidden_part.split(',')]
            # Check if ResMed value equals any forbidden value
            for forbidden in forbidden_values:
                if forbidden and resmed_val_str == forbidden:
                    return False  # Equals forbidden value - condition not satisfied
            return True  # Doesn't equal any forbidden value
    
    # Check "contains" condition (positive match)
    if 'contains' in condition_lower and 'does not contain' not in condition_lower:
        if is_empty:
            return False  # Empty values don't contain anything
        
        parts = condition_lower.split('contains')
        if len(parts) > 1:
            required_part = parts[1].split('in any item')[0].strip()
            # Handle comma-separated values
            required_values = [v.strip() for v in required_part.split(',')]
            # Check if ResMed value contains any required value
            for required in required_values:
                if required and required in resmed_val_str:
                    return True  # Contains required value
            return False  # Doesn't contain any required value
    
    # Check "equals" or "equal to" condition
    if 'equal to' in condition_lower or ('equals' in condition_lower and 'does not equal' not in condition_lower):
        if is_empty:
            return False  # Empty values don't equal anything
        
        if 'equal to' in condition_lower:
            parts = condition_lower.split('equal to')
        else:
            parts = condition_lower.split('equals')
        if len(parts) > 1:
            required_part = parts[1].split('in any item')[0].strip()
            # Handle comma-separated values
            required_values = [v.strip() for v in required_part.split(',')]
            # Check if ResMed value equals any required value
            for required in required_values:
                if required and resmed_val_str == required:
                    return True  # Equals required value
            return False  # Doesn't equal any required value
    
    return False


def check_value_against_conditions(resmed_value, rate_card_value, column_name, conditions_dict):
    """Check if ResMed value satisfies any condition for the rate card value.
    
    Returns:
        (is_valid, matching_condition) tuple
    """
    # Try to find column in conditions_dict (case-insensitive)
    column_key = None
    for key in conditions_dict.keys():
        if normalize_column_name(key) == normalize_column_name(column_name):
            column_key = key
            break
    
    if column_key is None:
        return False, None
    
    conditions = conditions_dict[column_key]
    rate_card_val_str = str(rate_card_value).lower() if pd.notna(rate_card_value) else ''
    
    # Handle both string and list formats for conditions
    # rate_card_processing.py returns conditions as a string (from cell comments)
    if isinstance(conditions, str):
        # Split string by newlines to get individual condition lines
        conditions_list = [line.strip() for line in conditions.split('\n') if line.strip()]
    elif isinstance(conditions, list):
        conditions_list = conditions
    else:
        # If it's neither string nor list, try to convert
        conditions_list = [str(conditions)]
    
    for condition_text in conditions_list:
        # Check if this condition applies to the rate card value
        # Format: "1. NAC: RATE_TYPE is empty..." or "NAC: RATE_TYPE is empty..."
        condition_lower = str(condition_text).lower()
        
        # Skip header lines
        if 'conditional rules' in condition_lower and ':' not in condition_text:
            continue
        
        # Check if condition is for this rate card value
        # Look for pattern like "NAC:" or "1. NAC:" or "1.NAC:"
        if rate_card_val_str:
            # Handle numbered conditions like "1. NAC:" or "1.NAC:"
            # Pattern: (optional number + dot + space) + rate_card_value + colon
            # Match patterns like "1. nac:", "1.nac:", "nac:"
            pattern = rf'(?:\d+\.\s*)?{re.escape(rate_card_val_str)}:'
            if re.search(pattern, condition_lower):
                # This condition applies to this rate card value
                is_valid = value_satisfies_condition(resmed_value, rate_card_value, condition_text)
                if is_valid:
                    return True, condition_text
    
    return False, None


def load_standardized_dataframes():
    """Load standardized dataframes from shipments.py output."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    excel_file = os.path.join(script_dir, "Standardized_Data.xlsx")
    
    if not os.path.exists(excel_file):
        print(f"Error: {excel_file} not found.")
        print("Please run shipments.py first to generate standardized data.")
        return None, None
    
    try:
        df_resmed = pd.read_excel(excel_file, sheet_name='ResMed (Standardized)')
        df_rate_card = pd.read_excel(excel_file, sheet_name='Rate Card (Standardized)')
        
        print(f"Loaded ResMed (Standardized): {df_resmed.shape}")
        print(f"Loaded Rate Card (Standardized): {df_rate_card.shape}")
        
        return df_resmed, df_rate_card
    except Exception as e:
        print(f"Error loading standardized data: {e}")
        return None, None


def find_common_columns(df_resmed, df_rate_card):
    """Find common columns between the two dataframes."""
    resmed_cols = set(df_resmed.columns)
    rate_card_cols = set(df_rate_card.columns)
    common_cols = sorted(list(resmed_cols & rate_card_cols))
    
    print(f"\nFound {len(common_cols)} common columns for matching:")
    for col in common_cols:
        print(f"  - {col}")
    
    return common_cols


def match_shipments_with_rate_card(df_etofs, df_filtered_rate_card, common_columns, conditions_dict=None):
    """Match ResMed shipments with Rate Card entries and identify discrepancies.
    
    Args:
        df_etofs: Shipment dataframe (ETOF or LC) from vocabular.py
        df_filtered_rate_card: Rate Card standardized dataframe from rate_card_processing.py
        common_columns: List of common column names
        conditions_dict: Dictionary of conditional rules from rate_card_processing.py
    """
    if conditions_dict is None:
        conditions_dict = {}
    
    # Create a copy to preserve all original columns
    df_etofs = df_etofs.copy()
    
    # Note: Country code extraction is already done in part1_etof_file_processing.py
    # No need to apply it again here as vocabular.py uses processed dataframes
    
    # Create mappings from normalized column names back to original column names
    etofs_original_to_normalized = {col: normalize_column_name(col) for col in df_etofs.columns}
    rate_card_original_to_normalized = {col: normalize_column_name(col) for col in df_filtered_rate_card.columns}
    
    etofs_normalized_to_original = {v: k for k, v in etofs_original_to_normalized.items()}
    rate_card_normalized_to_original = {v: k for k, v in rate_card_original_to_normalized.items()}
    
    # Get normalized versions of common columns
    common_columns_normalized = [normalize_column_name(col) for col in common_columns]
    
    # Get the original column names for the common normalized columns
    common_etofs_cols_original = [etofs_normalized_to_original[col_norm] 
                                   for col_norm in common_columns_normalized 
                                   if col_norm in etofs_normalized_to_original]
    common_rate_card_cols_original = [rate_card_normalized_to_original[col_norm] 
                                      for col_norm in common_columns_normalized 
                                      if col_norm in rate_card_normalized_to_original]
    
    print(f"\nMatching based on {len(common_columns_normalized)} common attributes:")
    print(common_columns_normalized)
    
    # Pre-calculate unique normalized values from df_filtered_rate_card for efficiency
    unique_rc_orig_countries_norm = set()
    unique_rc_dest_countries_norm = set()
    unique_rc_orig_dest_combinations = set()  # Store (origin, dest) tuples
    
    if 'Origin Country' in df_filtered_rate_card.columns:
        unique_rc_orig_countries_norm = set(df_filtered_rate_card['Origin Country'].apply(normalize_value).dropna())
    if 'Destination Country' in df_filtered_rate_card.columns:
        unique_rc_dest_countries_norm = set(df_filtered_rate_card['Destination Country'].apply(normalize_value).dropna())
    
    # Create set of (origin, destination) combinations from rate card
    if 'Origin Country' in df_filtered_rate_card.columns and 'Destination Country' in df_filtered_rate_card.columns:
        for _, rc_row in df_filtered_rate_card.iterrows():
            orig = normalize_value(rc_row.get('Origin Country'))
            dest = normalize_value(rc_row.get('Destination Country'))
            if orig and dest:
                unique_rc_orig_dest_combinations.add((orig, dest))
    
    # Initialize a new 'comment' column in df_etofs
    #df_etofs['Comments'] = ''
    
    # Iterate through each row of df_etofs
    for index_etofs, row_etofs in df_etofs.iterrows():
        comments_for_current_etofs_row = []
        
        # ===== STEP 1: Check Origin and Destination Countries =====
        # Find origin and destination country columns (handle variations)
        shipment_orig_country_norm = None
        shipment_dest_country_norm = None
        
        for col in ['Origin Country', 'origin country', 'OriginCountry']:
            if col in row_etofs:
                shipment_orig_country_norm = normalize_value(row_etofs[col])
                break
        
        for col in ['Destination Country', 'destination country', 'DestinationCountry']:
            if col in row_etofs:
                shipment_dest_country_norm = normalize_value(row_etofs[col])
                break
        
        # Check if origin country is missing
        if shipment_orig_country_norm is None:
            comments_for_current_etofs_row.append("origin country is missing")
        # Check if destination country is missing
        elif shipment_dest_country_norm is None:
            comments_for_current_etofs_row.append("destination country is missing")
        # If both present, check if they exist in rate card
        else:
            orig_missing = shipment_orig_country_norm not in unique_rc_orig_countries_norm
            dest_missing = shipment_dest_country_norm not in unique_rc_dest_countries_norm
            
            if orig_missing and dest_missing:
                comments_for_current_etofs_row.append("origin-destination are missing")
            elif orig_missing:
                orig_val = row_etofs.get('Origin Country', row_etofs.get('origin country', 'N/A'))
                comments_for_current_etofs_row.append(f"Origin country '{orig_val}' is missing")
            elif dest_missing:
                dest_val = row_etofs.get('Destination Country', row_etofs.get('destination country', 'N/A'))
                comments_for_current_etofs_row.append(f"Destination country '{dest_val}' is missing")
            else:
                # Both countries exist individually, check combination
                combination = (shipment_orig_country_norm, shipment_dest_country_norm)
                if combination not in unique_rc_orig_dest_combinations:
                    comments_for_current_etofs_row.append("Origin-Destination country combination is missing")
        
        # If country validation failed, skip matching and go to next row
        if comments_for_current_etofs_row:
            df_etofs.loc[index_etofs, 'comment'] = '\n'.join(comments_for_current_etofs_row)
            continue
        
        # ===== STEP 2: Check date within validity (working fine, keep as is) =====
        # Find Valid from/Valid to columns (once per row)
        valid_from_col = None
        valid_to_col = None
        for col in df_filtered_rate_card.columns:
            col_lower = str(col).lower()
            if 'valid' in col_lower and 'from' in col_lower:
                valid_from_col = col
            elif 'valid' in col_lower and 'to' in col_lower:
                valid_to_col = col
        
        # Find date column
        date_col = None
        date_value = None
        for col in ['SHIP_DATE', 'ship_date', 'Ship Date', 'ship date', 'SHIP DATE',
                   'Loading date', 'Loading Date', 'loading date', 'LOADING DATE']:
            if col in row_etofs:
                date_value = row_etofs[col]
                if pd.notna(date_value):
                    date_col = col
                    break
        
        # Check date validity before proceeding with matching
        date_invalid = False
        if date_col and date_value and valid_from_col and valid_to_col:
            # We'll check date validity for each match later, but first check if we should skip matching
            # For now, we'll proceed with matching and check dates during discrepancy identification
            pass
        
        # ===== STEP 3: Find rows with most common values =====
        # Only proceed with matching if countries are valid (no comments added yet)
        # Prepare normalized values for the current ETOFS row
        etofs_normalized_values = {
            col_norm: normalize_value(row_etofs[common_etofs_cols_original[i]])
            for i, col_norm in enumerate(common_columns_normalized)
            if i < len(common_etofs_cols_original) and common_etofs_cols_original[i] in row_etofs
        }
        
        max_matches = -1
        best_matching_rate_card_rows = []
        
        # Iterate through each row of df_filtered_rate_card
        for index_rate_card, row_rate_card in df_filtered_rate_card.iterrows():
            current_matches = 0
            
            # Prepare normalized values for the current Rate Card row
            rate_card_normalized_values = {
                col_norm: normalize_value(row_rate_card[common_rate_card_cols_original[i]])
                for i, col_norm in enumerate(common_columns_normalized)
                if i < len(common_rate_card_cols_original) and common_rate_card_cols_original[i] in row_rate_card
            }
            
            # Compare normalized values
            for col_norm in common_columns_normalized:
                if col_norm in etofs_normalized_values and col_norm in rate_card_normalized_values:
                    if etofs_normalized_values[col_norm] == rate_card_normalized_values[col_norm]:
                        current_matches += 1
            
            # Update best matches
            if current_matches > max_matches:
                max_matches = current_matches
                best_matching_rate_card_rows = [{'rate_card_row': row_rate_card.to_dict(), 'discrepancies': []}]
            elif current_matches == max_matches and current_matches > 0:  # Only append if there's at least one match
                best_matching_rate_card_rows.append({'rate_card_row': row_rate_card.to_dict(), 'discrepancies': []})
        
        # Check if more than 4 possible matches
        if len(best_matching_rate_card_rows) > 4:
            comments_for_current_etofs_row.append("Please recheck the shipment details. Too many possible rate lanes cab ne applied with changes.")
        
        # Only proceed with date validation and discrepancy checking if we have matches
        if len(best_matching_rate_card_rows) == 0:
            # No matches found - add comment and skip to next row
            if not comments_for_current_etofs_row:
                comments_for_current_etofs_row.append("No matching rate card entries found")
            df_etofs.loc[index_etofs, 'comment'] = '\n'.join(comments_for_current_etofs_row)
            continue
        
        # ===== STEP 2: Check date within validity (working fine, keep as is) =====
        # Find Valid from/Valid to columns (once per row)
        valid_from_col = None
        valid_to_col = None
        for col in df_filtered_rate_card.columns:
            col_lower = str(col).lower()
            if 'valid' in col_lower and 'from' in col_lower:
                valid_from_col = col
            elif 'valid' in col_lower and 'to' in col_lower:
                valid_to_col = col
        
        # Find date column
        date_col = None
        date_value = None
        for col in ['SHIP_DATE', 'ship_date', 'Ship Date', 'ship date', 'SHIP DATE',
                   'Loading date', 'Loading Date', 'loading date', 'LOADING DATE']:
            if col in row_etofs:
                date_value = row_etofs[col]
                if pd.notna(date_value):
                    date_col = col
                    break
        
        # Check date validity for all best matches first (before discrepancy checking)
        # If date is invalid for all matches, add comment and skip discrepancy checking
        if date_col and date_value and valid_from_col and valid_to_col:
            date_invalid_count = 0
            for best_match_info in best_matching_rate_card_rows:
                rate_card_row_dict = best_match_info['rate_card_row']
                valid_from = rate_card_row_dict.get(valid_from_col)
                valid_to = rate_card_row_dict.get(valid_to_col)
                
                if pd.notna(valid_from) and pd.notna(valid_to):
                    try:
                        date_dt = pd.to_datetime(date_value, errors='coerce')
                        valid_from_dt = pd.to_datetime(valid_from, errors='coerce')
                        valid_to_dt = pd.to_datetime(valid_to, errors='coerce')
                        
                        if pd.notna(date_dt) and pd.notna(valid_from_dt) and pd.notna(valid_to_dt):
                            if date_dt < valid_from_dt or date_dt > valid_to_dt:
                                date_invalid_count += 1
                    except Exception:
                        pass
            
            # If date is invalid for all matches, add comment and skip discrepancy checking
            if date_invalid_count == len(best_matching_rate_card_rows) and len(best_matching_rate_card_rows) > 0:
                comments_for_current_etofs_row.append(f"Date '{date_value}' is outside valid date range for all matching rate card entries")
                df_etofs.loc[index_etofs, 'comment'] = '\n'.join(comments_for_current_etofs_row)
                continue
        
        # ===== STEP 3: Identify discrepancies for best matching rows =====
        for match_idx, best_match_info in enumerate(best_matching_rate_card_rows):
            rate_card_row_dict = best_match_info['rate_card_row']
            discrepancies = []
            
            # Date validation (check for each match, but don't skip if only some are invalid)
            if date_col and date_value and valid_from_col and valid_to_col:
                valid_from = rate_card_row_dict.get(valid_from_col)
                valid_to = rate_card_row_dict.get(valid_to_col)
                
                if pd.notna(valid_from) and pd.notna(valid_to):
                    try:
                        # Convert dates to pandas datetime
                        date_dt = pd.to_datetime(date_value, errors='coerce')
                        valid_from_dt = pd.to_datetime(valid_from, errors='coerce')
                        valid_to_dt = pd.to_datetime(valid_to, errors='coerce')
                        
                        # Check if all dates were successfully parsed
                        if pd.notna(date_dt) and pd.notna(valid_from_dt) and pd.notna(valid_to_dt):
                            # Check if date is within valid range
                            if date_dt < valid_from_dt or date_dt > valid_to_dt:
                                discrepancies.append({
                                    'column': date_col,
                                    'etofs_value': date_value,
                                    'rate_card_value': f"Valid from: {valid_from} to {valid_to}",
                                    'condition': None,
                                    'type': 'date_range'
                                })
                    except Exception as e:
                        # If date parsing fails, skip this validation
                        pass
            
            for i, col_norm in enumerate(common_columns_normalized):
                if i >= len(common_etofs_cols_original) or i >= len(common_rate_card_cols_original):
                    continue
                    
                etofs_original_col = common_etofs_cols_original[i]
                rate_card_original_col = common_rate_card_cols_original[i]
                
                etofs_val = row_etofs.get(etofs_original_col)
                rate_card_val = rate_card_row_dict.get(rate_card_original_col)
                
                # Normalize values for consistent comparison for discrepancy reporting
                normalized_etofs_val = normalize_value(etofs_val)
                normalized_rate_card_val = normalize_value(rate_card_val)
                
                # Only report discrepancy if normalized values are different
                if normalized_etofs_val != normalized_rate_card_val:
                    # Check if ResMed value satisfies the condition for this rate card value
                    is_valid, matching_condition = check_value_against_conditions(
                        etofs_val, rate_card_val, etofs_original_col, conditions_dict
                    )
                    
                    if is_valid:
                        # Value satisfies condition - don't report as discrepancy
                        # Example: Rate Card has "NAC", condition says "NAC: RATE_TYPE is empty", 
                        #          ResMed has "nan" (empty) -> This is valid, no discrepancy
                        pass  # Skip this discrepancy
                    else:
                        # Value doesn't match and doesn't satisfy condition - report discrepancy
                        discrepancies.append({
                            'column': etofs_original_col,
                            'etofs_value': etofs_val,
                            'rate_card_value': rate_card_val,
                            'condition': matching_condition
                        })
            best_match_info['discrepancies'] = discrepancies
        
        # Check if more than 4 changes (discrepancies) across all matches
        total_discrepancies = sum(len(match['discrepancies']) for match in best_matching_rate_card_rows)
        if total_discrepancies > 5:
            comments_for_current_etofs_row.append("Please recheck the shipment details. Too many shipment details to update.")
        
        # Check if "please recheck the shipment details" is already in comments
        # If so, don't add discrepancy details - this will be the full comment
        has_recheck_comment = "Please recheck the shipment details" in '\n'.join(comments_for_current_etofs_row)
        
        # Add discrepancy details to comments only if "please recheck" is not present
        if not has_recheck_comment:
            for match_idx, best_match_info in enumerate(best_matching_rate_card_rows):
                discrepancies = best_match_info['discrepancies']
                if discrepancies:
                    rate_card_row_dict = best_match_info['rate_card_row']
                    lane_num = rate_card_row_dict.get('Lane #', rate_card_row_dict.get('Lane#', 'N/A'))
                    comments_for_current_etofs_row.append(f"Discrepancies for Match {match_idx+1}:")
                    for disc in discrepancies:
                        if disc.get('type') == 'date_range':
                            # Special formatting for date range discrepancies
                            comment = f"  {disc['column']}: Shipment value '{disc['etofs_value']}' is outside valid date range ({disc['rate_card_value']})"
                        else:
                            comment = f" {disc['column']}: Shipment value '{disc['etofs_value']}' needs to be changed to '{disc['rate_card_value']}'"
                            if disc.get('condition'):
                                comment += f" (Condition: {disc['condition'][:50]}...)"  # Show first 50 chars of condition
                        comments_for_current_etofs_row.append(comment)
        
        if comments_for_current_etofs_row:
            df_etofs.loc[index_etofs, 'comment'] = '\n'.join(comments_for_current_etofs_row)
        else:
            df_etofs.loc[index_etofs, 'comment'] = 'No discrepancies found for best match.'
    
    return df_etofs


def run_matching(rate_card_file_path=None):
    """
    Run the matching workflow to match shipments with rate card.
    
    Args:
        rate_card_file_path (str, optional): Path to rate card file relative to "input/" folder.
                                            If None, will try to find rate_card.xlsx or rate_card.xls in input folder.
    
    Returns:
        str: Path to the output file (Matched_Shipments_with.xlsx) if successful, None otherwise
    """
    import sys
    
    print("="*80)
    print("COMPARE AND FIND (CANF) - Match Shipments with Rate Card")
    print("="*80)
    
    # Step 1: Get rate card from part4_rate_card_processing.py
    print("\n1. Getting Rate Card from part4_rate_card_processing.py...")
    
    # If rate_card_file_path not provided, try to find it
    if rate_card_file_path is None:
        input_folder = "input"
        possible_names = ["rate_card.xlsx", "rate_card.xls"]
        for name in possible_names:
            full_path = os.path.join(input_folder, name)
            if os.path.exists(full_path):
                rate_card_file_path = name
                print(f"   Auto-detected rate card file: {rate_card_file_path}")
                break
        
        if rate_card_file_path is None:
            print(f"   [ERROR] Rate card file not found. Tried: {possible_names}")
            return None
    
    try:
        from part4_rate_card_processing import process_rate_card
        
        df_rate_card, rate_card_columns, rate_card_conditions = process_rate_card(rate_card_file_path)
        
        print(f"   Rate Card loaded: {df_rate_card.shape[0]} rows x {df_rate_card.shape[1]} columns")
        print(f"   Rate Card columns: {len(rate_card_columns)}")
        print(f"   Conditions loaded: {len(rate_card_conditions)} columns with conditions")
        if rate_card_conditions:
            print(f"   Columns with conditions: {list(rate_card_conditions.keys())}")
        
    except ImportError as e:
        print(f"   [ERROR] Could not import part4_rate_card_processing: {e}")
        print("   Please ensure part4_rate_card_processing.py is in the same directory.")
        return None
    except Exception as e:
        print(f"   [ERROR] Failed to process rate card: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # Step 2: Get dataframes from vocabular.py output (partly_df/vocabulary_mapping.xlsx)
    print("\n2. Loading Shipment dataframes from vocabular.py output...")
    
    # Step 2: Get dataframes from vocabular.py output (partly_df/vocabulary_mapping.xlsx)
    print("\n2. Loading Shipment dataframes from vocabular.py output...")
    
    # Vocabular output is stored in partly_df folder
    script_dir = os.path.dirname(os.path.abspath(__file__))
    vocabular_output_path = os.path.join(script_dir, "partly_df", "vocabulary_mapping.xlsx")
    
    if not os.path.exists(vocabular_output_path):
        print(f"   [ERROR] vocabulary_mapping.xlsx not found at: {vocabular_output_path}")
        print(f"   Please ensure vocabular.py has been run and the file exists in the partly_df folder.")
        return None
    
    print(f"   Found vocabulary_mapping.xlsx at: {vocabular_output_path}")

    
    try:
        # Read Excel file with all sheets
        excel_file = pd.ExcelFile(vocabular_output_path)
        sheet_names = excel_file.sheet_names
        print(f"   Found sheets in Excel file: {sheet_names}")
        
        etof_renamed = None
        lc_renamed = None
        origin_renamed = None
        
        # Read ETOF sheet if it exists
        if 'ETOF' in sheet_names:
            etof_renamed = pd.read_excel(vocabular_output_path, sheet_name='ETOF')
            print(f"   Loaded ETOF DataFrame: {etof_renamed.shape[0]} rows x {etof_renamed.shape[1]} columns")
        else:
            print(f"   [WARNING] ETOF sheet not found in vocabular_output.xlsx")
        
        # Read LC sheet if it exists
        if 'LC' in sheet_names:
            lc_renamed = pd.read_excel(vocabular_output_path, sheet_name='LC')
            print(f"   Loaded LC DataFrame: {lc_renamed.shape[0]} rows x {lc_renamed.shape[1]} columns")
        else:
            print(f"   [WARNING] LC sheet not found in vocabular_output.xlsx")
        
        # Read Origin sheet if it exists
        if 'Origin' in sheet_names:
            origin_renamed = pd.read_excel(vocabular_output_path, sheet_name='Origin')
            print(f"   Loaded Origin DataFrame: {origin_renamed.shape[0]} rows x {origin_renamed.shape[1]} columns")
        else:
            print(f"   [INFO] Origin sheet not found in vocabular_output.xlsx (optional)")
        
        if etof_renamed is None and lc_renamed is None:
            print(f"   [ERROR] No ETOF or LC dataframes found in vocabulary_mapping.xlsx")
            print(f"   Please ensure vocabular.py has been run and generated the Excel file with ETOF or LC sheets.")
            return None
    
    except FileNotFoundError:
        print(f"   [ERROR] File not found: {vocabular_output_path}")
        print(f"   Please run vocabular.py first to generate partly_df/vocabulary_mapping.xlsx")
        return None
    except Exception as e:
        print(f"   [ERROR] Failed to read vocabular_output.xlsx: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # Step 3: Select dataframe to process (LC if present, otherwise ETOF)
    print("\n3. Selecting shipment dataframe:")
    
    df_to_process = None
    shipment_type = None
    
    # Priority: LC first, then ETOF
    if lc_renamed is not None and not lc_renamed.empty:
        df_to_process = lc_renamed
        shipment_type = "LC"
        print(f"   Using LC DataFrame: {df_to_process.shape[0]} rows x {df_to_process.shape[1]} columns")
    elif etof_renamed is not None and not etof_renamed.empty:
        df_to_process = etof_renamed
        shipment_type = "ETOF"
        print(f"   LC not available, using ETOF DataFrame: {df_to_process.shape[0]} rows x {df_to_process.shape[1]} columns")
    else:
        print("\n   [ERROR] No LC or ETOF dataframes available to process!")
        print("   Please ensure vocabular.py has been run and generated LC or ETOF sheets.")
        sys.exit(1)
    
    # Step 4: Filter to only rows with values in ETOF # column
    print(f"\n4. Filtering rows with values in ETOF # column...")
    
    # Find ETOF # column (handle variations)
    etof_col = None
    etof_col_variations = ['ETOF #', 'ETOF#', 'etof #', 'etof#', 'ETOF', 'etof']
    
    for col in df_to_process.columns:
        col_normalized = str(col).strip()
        for variation in etof_col_variations:
            if col_normalized.lower() == variation.lower() or col_normalized.lower().replace(' ', '') == variation.lower().replace(' ', ''):
                etof_col = col
                break
        if etof_col:
            break
    
    if etof_col:
        print(f"   Found ETOF column: '{etof_col}'")
        initial_row_count = len(df_to_process)
        
        # Filter to keep only rows where ETOF # has a value (not null, not empty, not NaN)
        df_to_process = df_to_process[df_to_process[etof_col].notna()]
        df_to_process = df_to_process[df_to_process[etof_col].astype(str).str.strip() != '']
        df_to_process = df_to_process[df_to_process[etof_col].astype(str).str.lower() != 'nan']
        
        filtered_row_count = len(df_to_process)
        removed_rows = initial_row_count - filtered_row_count
        
        print(f"   Initial rows: {initial_row_count}")
        print(f"   Rows with ETOF # values: {filtered_row_count}")
        print(f"   Rows removed (no ETOF # value): {removed_rows}")
        
        if filtered_row_count == 0:
            print(f"\n   [ERROR] No rows remaining after filtering for ETOF # values!")
            print(f"   Please ensure the dataframe has rows with values in the ETOF # column.")
            return None
    else:
        print(f"   [WARNING] ETOF # column not found. Processing all rows.")
        print(f"   Searched for columns: {etof_col_variations}")
        print(f"   Available columns: {list(df_to_process.columns)}")
    
    # Step 5: Print input dataframe before matching
    print(f"\n5. Input {shipment_type} DataFrame before matching:")
    print(f"   Shape: {df_to_process.shape[0]} rows x {df_to_process.shape[1]} columns")
    print(f"   Columns: {list(df_to_process.columns)}")
    print(f"\n   First few rows:")
    print(df_to_process.head())
    
    # Step 6: Find common columns and match
    print(f"\n6. Finding common columns...")
    common_columns = find_common_columns(df_to_process, df_rate_card)
    
    if not common_columns:
        print("\nError: No common columns found between shipment and rate card dataframes.")
        return None
    
    # Step 7: Match shipments with rate card
    print("\n" + "="*80)
    print("MATCHING SHIPMENTS WITH RATE CARD")
    print("="*80)
    print("Note: Values will be validated against conditional rules before reporting discrepancies.")
    
    df_result = match_shipments_with_rate_card(df_to_process, df_rate_card, common_columns, rate_card_conditions)
    
    # Step 8: Reorder columns and save results
    print("\n8. Reordering columns and saving results...")
    # Handle Colab environment where __file__ is not defined
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # In Colab or interactive environments, use current working directory
        script_dir = os.getcwd()
    
    # Use absolute path to ensure it works even after directory changes
    output_file = os.path.abspath(os.path.join(script_dir, "Matched_Shipments_with.xlsx"))
    print(f"   Output file will be saved to: {output_file}")
    
    # Reorder columns: LC #, ETOF #, Shipment ID, Delivery Number, Carrier, Ship date, then others
    def reorder_columns(df):
        """Reorder columns with priority: LC #, ETOF #, Shipment ID, Delivery Number, Carrier, Ship date, then others."""
        if df is None or df.empty:
            return df
        
        # Find priority columns (handle variations)
        priority_columns = []
        other_columns = []
        
        # Define priority column patterns
        lc_patterns = ['LC #', 'LC#', 'lc #', 'lc#']
        etof_patterns = ['ETOF #', 'ETOF#', 'etof #', 'etof#']
        shipment_id_patterns = ['Shipment ID', 'ShipmentID', 'shipment id', 'shipmentid', 
                               'SHIPMENT_ID', 'SHIPMENT ID', 'Shipment', 'shipment', 'SHIPMENT']
        delivery_patterns = ['Delivery Number', 'DeliveryNumber', 'delivery number', 'deliverynumber',
                           'DELIVERY_NUMBER', 'Delivery', 'delivery', 'DELIVERY']
        carrier_patterns = ['Carrier', 'carrier', 'CARRIER']
        ship_date_patterns = ['SHIP_DATE', 'ship_date', 'Ship Date', 'ship date', 'SHIP DATE',
                             'Loading date', 'Loading Date', 'loading date', 'LOADING DATE']
        
        # Find and collect priority columns
        lc_col = None
        etof_col = None
        shipment_id_col = None
        delivery_col = None
        carrier_col = None
        ship_date_col = None
        
        for col in df.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '').replace('_', '')
            
            # Check for LC #
            if not lc_col:
                for pattern in lc_patterns:
                    if col_lower == pattern.lower().replace(' ', '') or col_str == pattern:
                        lc_col = col
                        break
            
            # Check for ETOF #
            if not etof_col:
                for pattern in etof_patterns:
                    if col_lower == pattern.lower().replace(' ', '') or col_str == pattern:
                        etof_col = col
                        break
            
            # Check for Shipment ID
            if not shipment_id_col:
                for pattern in shipment_id_patterns:
                    pattern_lower = pattern.lower().replace(' ', '').replace('_', '')
                    if col_lower == pattern_lower or col_str == pattern:
                        shipment_id_col = col
                        break
                # Also check if column contains "shipment" and "id"
                if not shipment_id_col and 'shipment' in col_lower and 'id' in col_lower:
                    shipment_id_col = col
            
            # Check for Delivery Number
            if not delivery_col:
                for pattern in delivery_patterns:
                    pattern_lower = pattern.lower().replace(' ', '').replace('_', '')
                    if col_lower == pattern_lower or col_str == pattern:
                        delivery_col = col
                        break
                # Also check if column contains "delivery" and "number"
                if not delivery_col and 'delivery' in col_lower and 'number' in col_lower:
                    delivery_col = col
            
            # Check for Carrier
            if not carrier_col:
                for pattern in carrier_patterns:
                    if col_lower == pattern.lower() or col_str == pattern:
                        carrier_col = col
                        break
            
            # Check for Ship date
            if not ship_date_col:
                for pattern in ship_date_patterns:
                    if col_lower == pattern.lower().replace(' ', '_') or col_str == pattern:
                        ship_date_col = col
                        break
        
        # Build ordered column list
        ordered_columns = []
        
        # Add priority columns in order: LC #, ETOF #, Shipment ID, Delivery Number, Carrier, Ship date
        if lc_col:
            ordered_columns.append(lc_col)
        if etof_col:
            ordered_columns.append(etof_col)
        if shipment_id_col:
            ordered_columns.append(shipment_id_col)
        if delivery_col:
            ordered_columns.append(delivery_col)
        if carrier_col:
            ordered_columns.append(carrier_col)
        if ship_date_col:
            ordered_columns.append(ship_date_col)
        
        # Add all other columns (excluding priority columns and comment)
        priority_set = {lc_col, etof_col, shipment_id_col, delivery_col, carrier_col, ship_date_col}
        for col in df.columns:
            if col not in priority_set and col != 'comment':
                ordered_columns.append(col)
        
        # Add comment column last
        if 'comment' in df.columns:
            ordered_columns.append('comment')
        
        # Reorder dataframe
        df_reordered = df[ordered_columns]
        
        print(f"   Column order: {ordered_columns[:7]}... (and {len(ordered_columns) - 7} more)")
        
        return df_reordered
    
    # Reorder result dataframe
    df_result_reordered = reorder_columns(df_result)
    
    try:
        from openpyxl import load_workbook
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df_result_reordered.to_excel(writer, sheet_name='Matched Shipments', index=False)
            
            # Apply formatting to the workbook
            workbook = writer.book
            
            # Format "Matched Shipments" sheet
            if 'Matched Shipments' in workbook.sheetnames:
                ws = workbook['Matched Shipments']
                
                # Style header row
                header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                header_font = Font(bold=True, color="FFFFFF", size=11)
                
                for cell in ws[1]:
                    cell.fill = header_fill
                    cell.font = header_font
                    cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                
                # Auto-adjust column widths
                for column in ws.columns:
                    max_length = 0
                    column_letter = get_column_letter(column[0].column)
                    
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    
                    # Set width with some padding, but cap at 50
                    adjusted_width = min(max_length + 2, 50)
                    ws.column_dimensions[column_letter].width = adjusted_width
                
                # Freeze header row
                ws.freeze_panes = 'A2'
                
                # Style comment column if it exists
                if 'comment' in df_result_reordered.columns:
                    comment_col_idx = list(df_result_reordered.columns).index('comment') + 1
                    comment_col_letter = get_column_letter(comment_col_idx)
                    
                    # Make comment column wider
                    ws.column_dimensions[comment_col_letter].width = 60
                    
                    # Wrap text in comment column
                    for row in ws.iter_rows(min_row=2, min_col=comment_col_idx, max_col=comment_col_idx):
                        for cell in row:
                            cell.alignment = Alignment(wrap_text=True, vertical="top")
            
        
        print(f"\n[SUCCESS] Results saved to: {output_file}")
        print(f"  - Sheet: Matched Shipments with comments (formatted)")
        print(f"\nTotal rows processed: {len(df_result)}")
        print(f"Total columns: {len(df_result.columns)} (reordered: LC #, ETOF #, Carrier, Ship date, then others)")
        
        # Show summary
        rows_with_comments = df_result[df_result['comment'] != '']
        rows_no_discrepancies = df_result[df_result['comment'] == 'No discrepancies found for best match.']
        print(f"  - Rows with comments/discrepancies: {len(rows_with_comments)}")
        print(f"  - Rows with no discrepancies: {len(rows_no_discrepancies)}")
        
    except ImportError:
        # Fallback if openpyxl formatting is not available
        print("   [WARNING] openpyxl formatting not available, saving without formatting...")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df_result_reordered.to_excel(writer, sheet_name='Matched Shipments', index=False)
        print(f"\n[SUCCESS] Results saved to: {output_file} (without formatting)")
    except PermissionError:
        print(f"\n[ERROR] Permission denied: Cannot write to {output_file}")
        print("   The file is likely open in Excel. Please close it and run again.")
        return None
    except Exception as e:
        print(f"\n[ERROR] Failed to save results: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    print(f"\n✅ Matching complete! Results saved to: {output_file}")
    print("="*80)
    
    return output_file


