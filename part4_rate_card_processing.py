import pandas as pd
import openpyxl
import os


def process_rate_card(file_path):
    """
    Process a Rate Card Excel file from the input folder.
    
    Args:
        file_path (str): Path to the file relative to the "input/" folder (e.g., "rate_card.xlsx")
    
    Returns:
        tuple: (dataframe, list of column names, conditions dictionary)
            - dataframe: Processed pandas DataFrame (filtered to black font columns)
            - list: List of column names in the processed dataframe
            - dict: Dictionary of conditions where keys are column names and values are condition text
    """
    # Construct full path from input folder
    input_folder = "input"
    full_path = os.path.join(input_folder, file_path)
    
    # Read the Excel file
    df_rate_card = pd.read_excel(full_path, sheet_name="Rate card", skiprows=2)
    
    # Find first column index (where data actually starts)
    first_column_index = None
    if df_rate_card is not None:
        for i, col in enumerate(df_rate_card.columns):
            if "nan" not in str(df_rate_card.iloc[0, i]).lower():
                first_column_index = i
                break
    
    if first_column_index is not None:
        df_rate_card = df_rate_card.iloc[:, :first_column_index]
    
    # Drop rows where the first column is NaN
    if df_rate_card is not None:
        df_rate_card.dropna(subset=[df_rate_card.columns[0]], inplace=True)
    
    # Set column names from first row
    new_columns = df_rate_card.iloc[0].tolist()
    df_rate_card.columns = new_columns
    df_rate_card = df_rate_card.iloc[1:]
    
    # Load the workbook to extract conditions and check font colors
    workbook = openpyxl.load_workbook(full_path, data_only=True)
    sheet = workbook["Rate card"]
    
    # Find the header row that contains "Currency"
    first_data_row_index = None
    currency_index = None
    
    for row_index in range(1, min(10, sheet.max_row + 1)):
        row = sheet[row_index]
        row_values = [cell.value for cell in row]
        if "Currency" in row_values:
            currency_index = row_values.index("Currency")
            first_data_row_index = row_index
            break
    
    black_font_values = []
    column_notes = {}  # Will store conditions/notes for each column
    
    if first_data_row_index is not None and currency_index is not None:
        # Access the data in this row
        first_data_row = sheet[first_data_row_index]
        first_data_values = [cell.value for cell in first_data_row]
        truncated_data_values = first_data_values[:currency_index]
        
        # Extract conditional rules/notes from COMMENTS in the header row cells
        header_row_index = first_data_row_index
        if header_row_index and header_row_index <= sheet.max_row:
            for i, col_name in enumerate(truncated_data_values, 1):
                if col_name:  # Only process non-empty column names
                    header_cell = sheet.cell(row=header_row_index, column=i)
                    
                    # Check for comments (where conditional rules are stored)
                    if header_cell.comment:
                        comment_text = header_cell.comment.text
                        if comment_text and comment_text.strip():
                            column_notes[col_name] = comment_text.strip()
                    
                    # Also check for cell value notes (in case some are stored as values in row 2)
                    if col_name not in column_notes:
                        notes_row_index = 2
                        if notes_row_index <= sheet.max_row:
                            note_cell = sheet.cell(row=notes_row_index, column=i)
                            if note_cell.value and str(note_cell.value).strip():
                                column_notes[col_name] = str(note_cell.value).strip()
        
        # Check font color to identify black font columns (required columns)
        for i, value in enumerate(truncated_data_values):
            if i < len(first_data_row):
                cell = first_data_row[i]
                font_color = "black"
                if cell.font and cell.font.color:
                    hex_color = cell.font.color.rgb
                    if hex_color is not None:
                        # Convert to string and handle different formats
                        hex_str = str(hex_color).upper()
                        # Remove 'FF' prefix if present (ARGB format)
                        if hex_str.startswith('FF') and len(hex_str) == 8:
                            hex_str = hex_str[2:]
                        
                        # Check if it's black
                        if hex_str == '000000' or hex_str == '00000000':
                            font_color = "black"
                        else:
                            # Check if it's a shade of grey (R, G, and B are close)
                            try:
                                if len(hex_str) == 6:
                                    r, g, b = int(hex_str[0:2], 16), int(hex_str[2:4], 16), int(hex_str[4:6], 16)
                                    # Check if it's a shade of grey (R, G, and B are close)
                                    if abs(r - g) < 10 and abs(g - b) < 10 and r > 0:  # Grey (not black, not white)
                                        font_color = "grey"
                                    else:
                                        font_color = "other non-black"  # For colors that are not black or grey
                            except (ValueError, IndexError):
                                pass
                
                if font_color == "black":
                    black_font_values.append(value)
    
    # Filter the DataFrame to keep only the columns whose names are in black_font_values
    if df_rate_card is not None and black_font_values:
        # Only include columns that actually exist in the dataframe
        available_columns = [col for col in black_font_values if col in df_rate_card.columns]
        if available_columns:
            df_filtered_rate_card = df_rate_card[available_columns]
        else:
            df_filtered_rate_card = df_rate_card
    else:
        df_filtered_rate_card = df_rate_card
    
    # Get list of column names
    column_names = df_filtered_rate_card.columns.tolist()
    
    # Create conditions dictionary (only for columns that exist in the filtered dataframe)
    conditions = {}
    for col_name in column_names:
        if col_name in column_notes:
            conditions[col_name] = column_notes[col_name]
    
    return df_filtered_rate_card, column_names, conditions


# Example usage
"""if __name__ == "__main__":
    rate_card_dataframe, rate_card_column_names, rate_card_conditions = process_rate_card("rate_card.xlsx")
    print("DataFrame:")
    print(rate_card_dataframe)
    print("\nColumn names:")
    print(rate_card_column_names)
    print("\nConditions:")
    print(rate_card_conditions)"""

#rate_card_dataframe, rate_card_column_names, rate_card_conditions = process_rate_card("rate_dairb.xlsx")
#print("DataFrame:")
#print(rate_card_dataframe)
# print("\nColumn names:")
#print(rate_card_column_names)
#print("\nConditions:")
#print(rate_card_conditions)