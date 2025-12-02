import pandas as pd
import os
import xml.etree.ElementTree as ET
from typing import Dict, List, Any
from pathlib import Path


def save_dataframe_to_excel(df, output_filename, folder_name="partly_df"):
    output_folder = Path(__file__).parent / folder_name
    output_folder.mkdir(exist_ok=True)
    df.to_excel(output_folder / output_filename, index=False, engine='openpyxl')


def process_origin_file(file_path, header_row=None, end_column=None):
    """
    Process a CSV, Excel, or EDI file from the input folder.
    
    Args:
        file_path (str): Path to the file relative to the "input/" folder 
                        (e.g., "origin_file.xlsx", "origin_file.csv", or "file.edi")
        header_row (int, optional): Row number (1-indexed, like Excel) where the header/column names are located.
                                   For example, header_row=15 means row 15 in Excel.
                                   Not used for EDI files (XML format).
        end_column (int, optional): Column number (1-indexed, like Excel) up to which to read (inclusive).
                                    For example, end_column=33 means read columns A through column 33.
                                    Applies to all file types.
    
    Returns:
        tuple: (dataframe, list of column names)
            - dataframe: Processed pandas DataFrame
            - list: List of column names in the processed dataframe
    """
    # Construct full path from input folder
    input_folder = "input"
    full_path = os.path.join(input_folder, file_path)
    
    # Check if file exists
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"File not found: {full_path}")
    
    # Get file extension
    file_extension = os.path.splitext(full_path)[1].lower()
    
    # Handle EDI files (XML format) separately
    if file_extension == '.edi':
        # EDI files are XML format, use special parser
        df, column_names = process_edi_file(file_path)
        # Apply end_column filter if specified
        if end_column is not None and end_column > 0:
            df = df.iloc[:, :end_column]
            column_names = df.columns.tolist()
        return df, column_names
    
    # For CSV and Excel files, header_row is required
    if header_row is None:
        raise ValueError("header_row is required for CSV and Excel files")
    
    # Convert 1-indexed header_row to 0-indexed for pandas (header_row=15 -> pandas header=14)
    pandas_header = header_row - 1
    
    # Read file based on extension
    if file_extension in ['.xlsx', '.xls']:
        df = pd.read_excel(full_path, header=pandas_header)
    elif file_extension == '.csv':
        df = pd.read_csv(full_path, header=pandas_header)
    else:
        raise ValueError(f"Unsupported file format: {file_extension}. Please use .xlsx, .xls, .csv, or .edi")
    
    # Select columns up to end_column (convert 1-indexed to 0-indexed, and make it inclusive)
    if end_column is not None and end_column > 0:
        # end_column is 1-indexed (like Excel), so column 33 means columns 0-32 (0-indexed)
        pandas_end_column = end_column
        df = df.iloc[:, :pandas_end_column]
    
    # Get list of column names
    column_names = df.columns.tolist()
    
    return df, column_names


def parse_edi_xml_to_dict(element, parent_path="", data_dict=None):
    """
    Recursively parse XML element and flatten it into a dictionary.
    
    Args:
        element: XML element to parse
        parent_path: Path prefix for nested elements
        data_dict: Dictionary to store parsed data
    
    Returns:
        Dictionary with flattened XML data
    """
    if data_dict is None:
        data_dict = {}
    
    # Get element tag name
    tag = element.tag
    if parent_path:
        current_path = f"{parent_path}_{tag}"
    else:
        current_path = tag
    
    # Get text content
    text = element.text.strip() if element.text and element.text.strip() else None
    
    # If element has text and no children, store it
    if text and len(list(element)) == 0:
        # If key exists, append to list or create list
        if current_path in data_dict:
            if not isinstance(data_dict[current_path], list):
                data_dict[current_path] = [data_dict[current_path]]
            data_dict[current_path].append(text)
        else:
            data_dict[current_path] = text
    
    # Process attributes
    for key, value in element.attrib.items():
        attr_path = f"{current_path}_{key}"
        data_dict[attr_path] = value
    
    # Recursively process children
    for child in element:
        parse_edi_xml_to_dict(child, current_path, data_dict)
    
    return data_dict


def process_edi_file(file_path):
    """
    Process an EDI (XML) file from the input folder and convert it to DataFrame.
    
    Args:
        file_path (str): Path to the file relative to the "input/" folder (e.g., "file.edi")
    
    Returns:
        tuple: (dataframe, list of column names)
            - dataframe: Processed pandas DataFrame with flattened XML data
            - list: List of column names in the processed dataframe
    """
    # Construct full path from input folder
    input_folder = "input"
    full_path = os.path.join(input_folder, file_path)
    
    # Check if file exists
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"File not found: {full_path}")
    
    # Parse XML file
    try:
        tree = ET.parse(full_path)
        root = tree.getroot()
    except ET.ParseError as e:
        raise ValueError(f"Error parsing EDI/XML file: {e}")
    
    # Extract all data from XML
    all_rows = []
    
    # Find InvoiceDetails elements (main data entries)
    invoice_details = root.findall('.//InvoiceDetails')
    
    if invoice_details:
        # Process each InvoiceDetails entry as a separate row
        for invoice_detail in invoice_details:
            row_data = parse_edi_xml_to_dict(invoice_detail, "InvoiceDetails")
            all_rows.append(row_data)
    else:
        # If no InvoiceDetails, process the entire Message section
        message = root.find('.//Message')
        if message is not None:
            row_data = parse_edi_xml_to_dict(message, "Message")
            all_rows.append(row_data)
        else:
            # Fallback: process entire root
            row_data = parse_edi_xml_to_dict(root, "")
            all_rows.append(row_data)
    
    # Also extract envelope and header information
    envelope = root.find('.//Envelope')
    invoice_header = root.find('.//InvoiceHeader')
    
    envelope_data = {}
    header_data = {}
    
    if envelope is not None:
        envelope_data = parse_edi_xml_to_dict(envelope, "Envelope")
    
    if invoice_header is not None:
        header_data = parse_edi_xml_to_dict(invoice_header, "InvoiceHeader")
    
    # Merge envelope and header data into each row
    for row in all_rows:
        row.update(envelope_data)
        row.update(header_data)
    
    # Create DataFrame
    if all_rows:
        # Convert list values to strings (join with semicolon) for DataFrame compatibility
        for row in all_rows:
            for key, value in row.items():
                if isinstance(value, list):
                    row[key] = "; ".join(str(v) for v in value)
        
        df = pd.DataFrame(all_rows)
    else:
        # Return empty DataFrame if no data found
        df = pd.DataFrame()
    
    # Get list of column names
    column_names = df.columns.tolist()
    
    return df, column_names


#if __name__ == "__main__":
#    origin_dataframe, origin_column_names = process_origin_file("file_dairb.xlsx", header_row=16, end_column=33)
#    save_dataframe_to_excel(origin_dataframe, "origin_processed_dairb.xlsx")
