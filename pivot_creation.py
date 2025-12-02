import pandas as pd
import os
import glob

def clean_comment_line(line):
    """Clean a single comment line by normalizing date comments and removing unwanted lines."""
    if pd.isna(line) or line == '':
        return None
    line_stripped = str(line).strip()
    
    # Remove 'Discrepancies for Match' lines
    if line_stripped.startswith('Discrepancies for Match'):
        return None
    
    # Normalize date comments to base form (remove specific date value for counting)
    # Pattern: "Date 'YYYYMMDD' is outside valid date range..." -> "Date is outside valid date range..."
    import re
    if "Date '" in line_stripped and "is outside valid date range" in line_stripped:
        # Always return the base form for date comments
        if "for all matching rate card entries" in line_stripped:
            return "Date is outside valid date range for all matching rate card entries"
        else:
            # For other date comment variations, remove the date part
            cleaned_line = re.sub(r"Date '[^']+'", "Date", line_stripped)
            return cleaned_line
    
    return line_stripped

def update_canf_file(matching_output_file=None,
                     shipper_value=None):
    """
    Process matching.py output file and add pivot data tab to the original file.

    Args:
        matching_output_file: Path to the matching.py output Excel file (Matched_Shipments_with.xlsx)
        shipper_value: Shipper value to add to the new tab
    """
    try:
        # Find matching.py output file
        if matching_output_file is None:
            # Try to find Matched_Shipments_with.xlsx in common locations
            script_dir = os.path.dirname(os.path.abspath(__file__))
            possible_locations = [
                os.path.join(script_dir, "Matched_Shipments_with.xlsx"),
                os.path.join(os.path.dirname(script_dir), "test folder", "Matched_Shipments_with.xlsx"),
                "Matched_Shipments_with.xlsx"
            ]
            
            for loc in possible_locations:
                if os.path.exists(loc):
                    matching_output_file = loc
                    print(f"Found matching output file: {matching_output_file}")
                    break
            
            if matching_output_file is None:
                print(f"Error: Could not find Matched_Shipments_with.xlsx in any of these locations:")
                for loc in possible_locations:
                    print(f"  - {loc}")
                return False
        elif not os.path.exists(matching_output_file):
            print(f"Error: Matching output file not found: {matching_output_file}")
            return False

        print(f"Reading matching output file: {matching_output_file}")

        # Read the "Matched Shipments" sheet from matching.py output
        try:
            df_etofs = pd.read_excel(matching_output_file, sheet_name='Matched Shipments')
            print(f"Loaded {len(df_etofs)} rows from 'Matched Shipments' sheet")
        except Exception as e:
            print(f"Error reading 'Matched Shipments' sheet: {e}")
            # Try reading the first sheet as fallback
            df_etofs = pd.read_excel(matching_output_file)
            print(f"Loaded {len(df_etofs)} rows from first sheet")

        # Prepare data for Google Sheets: Carrier, Cause of CANF, Amount
        # Check for 'comment' column (matching.py uses 'comment', not 'Comments')
        comment_col = None
        if 'comment' in df_etofs.columns:
            comment_col = 'comment'
        elif 'Comments' in df_etofs.columns:
            comment_col = 'Comments'
        
        if 'Carrier' in df_etofs.columns and comment_col:
            # Create cross-product of Carrier and cleaned Comments
            # First, merge Carrier with cleaned comments
            carrier_cause_df = df_etofs[['Carrier', comment_col]].copy()
            carrier_cause_df.rename(columns={comment_col: 'Comments'}, inplace=True)
            
            # Expand comments: split multi-line comments into separate rows
            # Each line becomes a separate row for proper grouping
            expanded_rows = []
            for idx, row in carrier_cause_df.iterrows():
                carrier = row['Carrier']
                comment = row['Comments']
                
                if pd.isna(comment) or comment == '':
                    continue
                
                # Split comment into lines and clean each line
                comment_lines = str(comment).split('\n')
                for line in comment_lines:
                    # Clean the line (normalize date comments, remove "Discrepancies for Match")
                    cleaned_line = clean_comment_line(line)
                    
                    # Only add if cleaned_line is not None and not empty
                    if cleaned_line and cleaned_line.strip() != '':
                        expanded_rows.append({
                            'Carrier': carrier,
                            'Cause of CANF': cleaned_line
                        })
            
            # Create dataframe from expanded rows
            if expanded_rows:
                expanded_df = pd.DataFrame(expanded_rows)
                
                # Count occurrences of each Carrier + Cause combination
                google_sheets_data = expanded_df.groupby(['Carrier', 'Cause of CANF']).size().reset_index(name='Amount')
                
                # Add shipper value column to the pivot data
                if shipper_value:
                    google_sheets_data['Shipper Value'] = shipper_value
                else:
                    google_sheets_data['Shipper Value'] = 'Not provided'
                
                # Reorder columns: Shipper Value, Carrier, Cause of CANF, Amount
                google_sheets_data = google_sheets_data[['Shipper Value', 'Carrier', 'Cause of CANF', 'Amount']]
                google_sheets_data = google_sheets_data.sort_values(['Carrier', 'Cause of CANF'])
            else:
                google_sheets_data = pd.DataFrame(columns=['Shipper Value', 'Carrier', 'Cause of CANF', 'Amount'])

            print(f"\nPrepared {len(google_sheets_data)} Carrier-Cause combinations.")

            # Update the original matching.py output file by adding new sheets
            try:
                # Read all existing sheets from the matching output file
                excel_file = pd.ExcelFile(matching_output_file)
                existing_sheets = {}
                
                for sheet_name in excel_file.sheet_names:
                    existing_sheets[sheet_name] = pd.read_excel(matching_output_file, sheet_name=sheet_name)
                    print(f"  Preserved existing sheet: '{sheet_name}' ({len(existing_sheets[sheet_name])} rows)")
                
                # Save all sheets back to the original file (existing sheets + new sheets)
                try:
                    from openpyxl import load_workbook
                    from openpyxl.styles import Font, PatternFill, Alignment
                    from openpyxl.utils import get_column_letter
                    FORMATTING_AVAILABLE = True
                except ImportError:
                    FORMATTING_AVAILABLE = False
                
                with pd.ExcelWriter(matching_output_file, engine='openpyxl') as writer:
                    # Write all existing sheets
                    for sheet_name, sheet_df in existing_sheets.items():
                        sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    # Add new Pivot Data sheet
                    google_sheets_data = google_sheets_data.sort_values(['Carrier', 'Cause of CANF']).reset_index(drop=True)
                    google_sheets_data.to_excel(writer, sheet_name='Pivot Data', index=False)
                    
                    # Apply formatting if available
                    if FORMATTING_AVAILABLE:
                        workbook = writer.book
                        
                        # Format all sheets
                        for sheet_name in workbook.sheetnames:
                            ws = workbook[sheet_name]
                            
                            # Determine header color based on sheet name
                            if sheet_name == 'Matched Shipments':
                                header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                            elif sheet_name == 'Rate Card Reference':
                                header_fill = PatternFill(start_color="70AD47", end_color="70AD47", fill_type="solid")
                            elif sheet_name == 'Pivot Data':
                                header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                            else:
                                # Default header color for other sheets
                                header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
                            
                            header_font = Font(bold=True, color="FFFFFF", size=11)
                            
                            # Style header row
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
                            
                            # Special formatting for specific sheets
                            if sheet_name == 'Pivot Data':
                                # Make "Cause of CANF" column wider for better readability
                                if 'Cause of CANF' in google_sheets_data.columns:
                                    cause_col_idx = list(google_sheets_data.columns).index('Cause of CANF') + 1
                                    cause_col_letter = get_column_letter(cause_col_idx)
                                    ws.column_dimensions[cause_col_letter].width = 60
                                    
                                    # Wrap text in Cause of CANF column
                                    for row in ws.iter_rows(min_row=2, min_col=cause_col_idx, max_col=cause_col_idx):
                                        for cell in row:
                                            cell.alignment = Alignment(wrap_text=True, vertical="top")
                            
                            elif sheet_name == 'Matched Shipments':
                                # Make comment column wider and wrap text
                                if 'comment' in existing_sheets.get('Matched Shipments', pd.DataFrame()).columns:
                                    comment_col_idx = list(existing_sheets['Matched Shipments'].columns).index('comment') + 1
                                    comment_col_letter = get_column_letter(comment_col_idx)
                                    ws.column_dimensions[comment_col_letter].width = 60
                                    
                                    # Wrap text in comment column
                                    for row in ws.iter_rows(min_row=2, min_col=comment_col_idx, max_col=comment_col_idx):
                                        for cell in row:
                                            cell.alignment = Alignment(wrap_text=True, vertical="top")
                
                print(f"\nSuccessfully updated file '{matching_output_file}'!")
                print(f"  - Preserved {len(existing_sheets)} existing sheet(s)")
                print(f"  - Added 'Pivot Data' sheet with {len(google_sheets_data)} rows")

            except Exception as e:
                print(f"Error updating file: {str(e)}")
                import traceback
                traceback.print_exc()
                return False
        else:
            print(f"Carrier or Comments column not found. Available columns: {list(df_etofs.columns)}")
            print("Skipping Excel file update.")
            return False

        return True

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

# Example usage:
#if __name__ == "__main__":
    # USER INPUT: Provide shipper value here
#    SHIPPER_VALUE = "Your Shipper Value Here"  # Change this to your shipper value
    
#    update_canf_file(
 #       matching_output_file=None,  # Will auto-detect Matched_Shipments_with.xlsx
 #       shipper_value=SHIPPER_VALUE
 #   )

