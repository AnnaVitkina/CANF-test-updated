import os
import sys
import gradio as gr

# Auto-detect and add script directory to Python path (for Colab compatibility)
def setup_python_path():
    """Setup Python path to include the script directory for imports."""
    try:
        # Try to get script directory from __file__
        if '__file__' in globals():
            script_dir = os.path.dirname(os.path.abspath(__file__))
        else:
            # In Colab or interactive environments, try common locations
            script_dir = os.getcwd()
            # Check if we're in a cloned repository
            possible_paths = [
                '/content/CANF-test-updated',
                '/content/CANF-test-updated/test folder',
                os.path.join(os.getcwd(), 'CANF-test-updated'),
                os.path.join(os.getcwd(), 'CANF-test-updated', 'test folder'),
            ]
            for path in possible_paths:
                if os.path.exists(path) and os.path.isdir(path):
                    # Check if vocabular.py exists there
                    vocabular_path = os.path.join(path, 'vocabular.py')
                    if os.path.exists(vocabular_path):
                        script_dir = path
                        break
        
        # Add script directory to Python path if not already there
        if script_dir and script_dir not in sys.path:
            sys.path.insert(0, script_dir)
            print(f"📁 Added to Python path: {script_dir}")
        
        # Also try adding 'test folder' subdirectory if it exists
        test_folder_path = os.path.join(script_dir, 'test folder')
        if os.path.exists(test_folder_path) and test_folder_path not in sys.path:
            sys.path.insert(0, test_folder_path)
            print(f"📁 Added to Python path: {test_folder_path}")
            
    except Exception as e:
        print(f"⚠️ Warning: Could not auto-detect script directory: {e}")
        print(f"   Current working directory: {os.getcwd()}")
        print(f"   Please ensure all Python files are in the same directory")

# Run setup when module is imported
setup_python_path()

def run_full_workflow_gradio(rate_card_file, etof_file, lc_file, origin_file, order_files, shipper_id, 
                             origin_header_row=None, origin_end_column=None, ignore_rate_card_columns=None):
    """
    Main workflow for use in Gradio, designed for Google Colab.
    Accepts uploaded files and user input; returns a downloadable file and status messages.
    """
    import shutil
    import tempfile
    from io import StringIO
    import sys
    
    # Capture all print statements and errors
    status_messages = []
    errors = []
    warnings = []
    
    def log_status(msg, level="info"):
        """Log status messages with different levels"""
        timestamp = ""
        try:
            from datetime import datetime
            timestamp = datetime.now().strftime("%H:%M:%S")
        except:
            pass
        
        formatted_msg = f"[{timestamp}] {msg}"
        status_messages.append(formatted_msg)
        
        if level == "error":
            errors.append(msg)
        elif level == "warning":
            warnings.append(msg)
        
        # Also print to console
        print(formatted_msg)
    
    # Redirect stdout to capture print statements
    class StatusCapture:
        def __init__(self):
            self.buffer = []
        
        def write(self, s):
            if s.strip():
                log_status(s.strip())
        
        def flush(self):
            pass

    # Handle file input (Gradio may give strings or tempfile paths)
    def _handle_upload(uploaded, allow_multiple=False):
        if uploaded is None:
            return None if not allow_multiple else []
        # Handle list of files (for multiple file uploads)
        if isinstance(uploaded, list):
            if not allow_multiple:
                # If single file expected but got list, return first item
                return _handle_upload(uploaded[0] if uploaded else None, allow_multiple=False)
            # Process each file in the list
            result = []
            for item in uploaded:
                if item is None:
                    continue
                if hasattr(item, "name"):
                    result.append(item.name)
                elif isinstance(item, str):
                    result.append(item)
            return result if result else []
        # Handle single file
        if hasattr(uploaded, "name"):
            return uploaded.name
        if isinstance(uploaded, str):
            return uploaded
        return None if not allow_multiple else []

    # Convert all filepaths to correct types
    rate_card_path = _handle_upload(rate_card_file)
    etof_path = _handle_upload(etof_file)
    lc_path = _handle_upload(lc_file, allow_multiple=True)  # Allow multiple LC files
    origin_path = _handle_upload(origin_file)
    order_files_path = _handle_upload(order_files)

    # Validate required fields: rate_card, etof, and shipper_id are required
    if not rate_card_path:
        error_msg = "❌ Error: Rate Card File is required."
        log_status(error_msg, "error")
        return None, error_msg
    if not etof_path:
        error_msg = "❌ Error: ETOF File is required."
        log_status(error_msg, "error")
        return None, error_msg
    if not shipper_id or not shipper_id.strip():
        error_msg = "❌ Error: Shipper ID is required."
        log_status(error_msg, "error")
        return None, error_msg
    
    log_status("✅ Validation passed. Starting workflow...", "info")

    # Create output and input directories for results
    # Handle Colab environment where __file__ is not defined
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # In Colab or interactive environments, use current working directory
        script_dir = os.getcwd()
    output_dir = os.path.join(script_dir, "output")
    input_dir = os.path.join(script_dir, "input")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(input_dir, exist_ok=True)
    
    # Use Gradio workspace or /content/ for outputs (fallback to output directory)
    result_xlsx_path = os.path.join(output_dir, "Result.xlsx")

    # Copy uploaded files to input directory with standard names
    # This is necessary because processing functions expect files in "input/" folder
    rate_card_filename = None
    etof_filename = None
    origin_filename = None
    order_files_filename = None
    
    if rate_card_path:
        # Preserve original extension
        rate_card_ext = os.path.splitext(rate_card_path)[1] or ".xlsx"
        rate_card_filename = f"rate_card{rate_card_ext}"
        input_rate_card_path = os.path.join(input_dir, rate_card_filename)
        shutil.copy2(rate_card_path, input_rate_card_path)
        log_status(f"✓ Copied rate card to: {input_rate_card_path}", "info")
        if not os.path.exists(input_rate_card_path):
            error_msg = f"❌ Error: Failed to copy rate card file. Source: {rate_card_path}, Destination: {input_rate_card_path}"
            log_status(error_msg, "error")
            return None, error_msg
    
    if etof_path:
        # Preserve original extension
        etof_ext = os.path.splitext(etof_path)[1] or ".xlsx"
        etof_filename = f"etof_file{etof_ext}"
        input_etof_path = os.path.join(input_dir, etof_filename)
        shutil.copy2(etof_path, input_etof_path)
        log_status(f"✓ Copied ETOF to: {input_etof_path}", "info")
        if not os.path.exists(input_etof_path):
            error_msg = f"❌ Error: Failed to copy ETOF file. Source: {etof_path}, Destination: {input_etof_path}"
            log_status(error_msg, "error")
            return None, error_msg
    
    # Handle multiple LC files
    lc_filenames = []
    if lc_path:
        # Handle both single file and list of files
        lc_files_list = lc_path if isinstance(lc_path, list) else [lc_path]
        
        for idx, lc_file_path in enumerate(lc_files_list):
            if lc_file_path:
                # Preserve original filename for LC files
                lc_filename = os.path.basename(lc_file_path)
                # If multiple files, ensure unique names
                if len(lc_files_list) > 1:
                    name, ext = os.path.splitext(lc_filename)
                    lc_filename = f"{name}_{idx+1}{ext}" if lc_filename in lc_filenames else lc_filename
                
                input_lc_path = os.path.join(input_dir, lc_filename)
                shutil.copy2(lc_file_path, input_lc_path)
                lc_filenames.append(lc_filename)
                log_status(f"✓ Copied LC file {idx+1}/{len(lc_files_list)}: {lc_filename}", "info")
                if not os.path.exists(input_lc_path):
                    log_status(f"⚠️ Warning: Failed to verify LC file copy. Source: {lc_file_path}, Destination: {input_lc_path}", "warning")
        
        log_status(f"✓ Total {len(lc_filenames)} LC file(s) copied to input folder", "info")
    
    if origin_path:
        # Get original filename extension
        origin_ext = os.path.splitext(origin_path)[1] or ".xlsx"
        origin_filename = f"origin_file{origin_ext}"
        input_origin_path = os.path.join(input_dir, origin_filename)
        shutil.copy2(origin_path, input_origin_path)
        log_status(f"✓ Copied origin file to: {input_origin_path}", "info")
        if not os.path.exists(input_origin_path):
            log_status(f"⚠️ Warning: Failed to verify origin file copy. Source: {origin_path}, Destination: {input_origin_path}", "warning")
    
    if order_files_path:
        # Get original filename extension
        order_ext = os.path.splitext(order_files_path)[1] or ".xlsx"
        order_files_filename = f"order_files{order_ext}"
        input_order_files_path = os.path.join(input_dir, order_files_filename)
        shutil.copy2(order_files_path, input_order_files_path)
        log_status(f"✓ Copied order files to: {input_order_files_path}", "info")
        if not os.path.exists(input_order_files_path):
            log_status(f"⚠️ Warning: Failed to verify order files copy. Source: {order_files_path}, Destination: {input_order_files_path}", "warning")

    # Change to script directory so "input/" folder is relative to it
    original_cwd = os.getcwd()
    try:
        os.chdir(script_dir)
        
        # --- PART 1: ETOF Processing (Optionally run, but not mandatory in Colab GUI) ---
        try:
            from part1_etof_file_processing import process_etof_file
            if etof_filename:
                # Verify file exists before processing
                etof_full_path = os.path.join("input", etof_filename)
                if not os.path.exists(etof_full_path):
                    log_status(f"❌ Error: ETOF file not found at: {etof_full_path}", "error")
                    log_status(f"Current directory: {os.getcwd()}", "info")
                    log_status(f"Input directory contents: {os.listdir('input') if os.path.exists('input') else 'input folder does not exist'}", "info")
                else:
                    log_status(f"\n{'='*80}", "info")
                    log_status(f"Processing ETOF file: {etof_filename}", "info")
                    log_status(f"{'='*80}", "info")
                    etof_df, etof_columns = process_etof_file(etof_filename)
                    log_status(f"\nETOF Processing Results:", "info")
                    log_status(f"  - Shape: {etof_df.shape[0]} rows x {etof_df.shape[1]} columns", "info")
                    log_status(f"  - Columns ({len(etof_columns)}): {etof_columns[:10]}{'...' if len(etof_columns) > 10 else ''}", "info")
                    log_status(f"\nFirst 5 rows of ETOF data:", "info")
                    log_status(etof_df.head().to_string(), "info")
                    log_status(f"{'='*80}\n", "info")
        except Exception as e:
            log_status(f"⚠️ Warning: ETOF processing failed: {e}", "warning")
            import traceback
            error_trace = traceback.format_exc()
            log_status(f"Traceback: {error_trace}", "error")

        # --- PART 2: LC Processing ---
        try:
            from part2_lc_processing import process_lc_input
            if lc_filenames:
                log_status(f"\n{'='*80}", "info")
                log_status(f"Processing LC file(s): {len(lc_filenames)} file(s)", "info")
                log_status(f"  - Files: {', '.join(lc_filenames)}", "info")
                log_status(f"{'='*80}", "info")
                # Pass list of filenames if multiple, single filename if one
                lc_input_param = lc_filenames if len(lc_filenames) > 1 else lc_filenames[0]
                lc_df, lc_columns = process_lc_input(lc_input_param, recursive=False)
                log_status(f"\nLC Processing Results:", "info")
                log_status(f"  - Shape: {lc_df.shape[0]} rows x {lc_df.shape[1]} columns", "info")
                log_status(f"  - Columns ({len(lc_columns)}): {lc_columns[:10]}{'...' if len(lc_columns) > 10 else ''}", "info")
                log_status(f"\nFirst 5 rows of LC data:", "info")
                log_status(lc_df.head().to_string(), "info")
                log_status(f"{'='*80}\n", "info")
        except Exception as e:
            log_status(f"⚠️ Warning: LC processing failed: {e}", "warning")
            import traceback
            error_trace = traceback.format_exc()
            log_status(f"Traceback: {error_trace}", "error")

        # --- PART 3: Origin File Processing ---
        try:
            from part3_origin_file_processing import process_origin_file
            if origin_filename:
                # Convert header_row and end_column to integers if provided
                header_row_int = None
                end_column_int = None
                if origin_header_row is not None:
                    try:
                        header_row_int = int(origin_header_row)
                    except (ValueError, TypeError):
                        header_row_int = None
                if origin_end_column is not None:
                    try:
                        end_column_int = int(origin_end_column)
                    except (ValueError, TypeError):
                        end_column_int = None
                log_status(f"\n{'='*80}", "info")
                log_status(f"Processing Origin file: {origin_filename}", "info")
                log_status(f"  - Header row: {header_row_int}", "info")
                log_status(f"  - End column: {end_column_int}", "info")
                log_status(f"{'='*80}", "info")
                origin_df, origin_columns = process_origin_file(origin_filename, header_row=header_row_int, end_column=end_column_int)
                log_status(f"\nOrigin Processing Results:", "info")
                log_status(f"  - Shape: {origin_df.shape[0]} rows x {origin_df.shape[1]} columns", "info")
                log_status(f"  - Columns ({len(origin_columns)}): {origin_columns[:10]}{'...' if len(origin_columns) > 10 else ''}", "info")
                log_status(f"\nFirst 5 rows of Origin data:", "info")
                log_status(origin_df.head().to_string(), "info")
                log_status(f"{'='*80}\n", "info")
        except Exception as e:
            log_status(f"⚠️ Warning: Origin file processing failed: {e}", "warning")
            import traceback
            error_trace = traceback.format_exc()
            log_status(f"Traceback: {error_trace}", "error")

        # --- PART 4: Rate Card Processing ---
        try:
            from part4_rate_card_processing import process_rate_card
            if rate_card_filename:
                # Verify file exists before processing
                rate_card_full_path = os.path.join("input", rate_card_filename)
                if not os.path.exists(rate_card_full_path):
                    log_status(f"❌ Error: Rate card file not found at: {rate_card_full_path}", "error")
                    log_status(f"Current directory: {os.getcwd()}", "info")
                    log_status(f"Input directory contents: {os.listdir('input') if os.path.exists('input') else 'input folder does not exist'}", "info")
                else:
                    log_status(f"\n{'='*80}", "info")
                    log_status(f"Processing Rate Card file: {rate_card_filename}", "info")
                    log_status(f"{'='*80}", "info")
                    rate_card_df, rate_card_columns, rate_card_conditions = process_rate_card(rate_card_filename)
                    log_status(f"\nRate Card Processing Results:", "info")
                    log_status(f"  - Shape: {rate_card_df.shape[0]} rows x {rate_card_df.shape[1]} columns", "info")
                    log_status(f"  - Columns ({len(rate_card_columns)}): {rate_card_columns[:10]}{'...' if len(rate_card_columns) > 10 else ''}", "info")
                    log_status(f"  - Conditions: {len(rate_card_conditions)} columns with conditions", "info")
                    log_status(f"\nFirst 5 rows of Rate Card data:", "info")
                    log_status(rate_card_df.head().to_string(), "info")
                    log_status(f"{'='*80}\n", "info")
        except Exception as e:
            log_status(f"⚠️ Warning: Rate card processing failed: {e}", "warning")
            import traceback
            error_trace = traceback.format_exc()
            log_status(f"Traceback: {error_trace}", "error")

        # --- PART 7: Optional Order-LC-ETOF Mapping ---
        try:
            from part7_optional_order_lc_etof_mapping import process_order_lc_etof_mapping
            if lc_filenames and etof_filename:
                log_status(f"\n{'='*80}", "info")
                log_status(f"Processing Order-LC-ETOF Mapping", "info")
                log_status(f"  - LC file(s): {len(lc_filenames)} file(s) - {', '.join(lc_filenames)}", "info")
                log_status(f"  - ETOF file: {etof_filename}", "info")
                log_status(f"  - Order files: {order_files_filename if order_files_filename else 'None'}", "info")
                log_status(f"{'='*80}", "info")
                # Pass list of filenames if multiple, single filename if one
                lc_input_param = lc_filenames if len(lc_filenames) > 1 else lc_filenames[0]
                lc_mapped_df, lc_mapped_columns = process_order_lc_etof_mapping(
                    lc_input_path=lc_input_param, 
                    etof_path=etof_filename,
                    order_files_path=order_files_filename
                )
                log_status(f"\nOrder-LC-ETOF Mapping Results:", "info")
                log_status(f"  - Shape: {lc_mapped_df.shape[0]} rows x {lc_mapped_df.shape[1]} columns", "info")
                log_status(f"  - Columns ({len(lc_mapped_columns)}): {lc_mapped_columns[:10]}{'...' if len(lc_mapped_columns) > 10 else ''}", "info")
                log_status(f"\nFirst 5 rows of mapped LC data:", "info")
                log_status(lc_mapped_df.head().to_string(), "info")
                log_status(f"{'='*80}\n", "info")
        except Exception as e:
            log_status(f"⚠️ Warning: Order-LC-ETOF mapping failed: {e}", "warning")
            import traceback
            error_trace = traceback.format_exc()
            log_status(f"Traceback: {error_trace}", "error")

        # --- VOCABULARY MAPPING ---
        # Try importing vocabular with multiple fallback strategies
        vocabular_imported = False
        
        # Strategy 1: Direct import
        try:
            from vocabular import map_and_rename_columns
            vocabular_imported = True
            log_status("✓ Successfully imported vocabular module", "info")
        except ImportError:
            pass
        
        # Strategy 2: Try adding current directory and common Colab paths
        if not vocabular_imported:
            paths_to_try = [
                os.getcwd(),
                '/content/CANF-test-updated',
                '/content/CANF-test-updated/test folder',
                os.path.join(os.getcwd(), 'CANF-test-updated'),
                os.path.join(os.getcwd(), 'CANF-test-updated', 'test folder'),
            ]
            
            for path in paths_to_try:
                if path and os.path.exists(path):
                    vocabular_file = os.path.join(path, 'vocabular.py')
                    if os.path.exists(vocabular_file):
                        if path not in sys.path:
                            sys.path.insert(0, path)
                            log_status(f"   Added to path: {path}", "info")
                        try:
                            from vocabular import map_and_rename_columns
                            vocabular_imported = True
                            log_status(f"✓ Successfully imported vocabular from: {path}", "info")
                            break
                        except ImportError:
                            continue
        
        # Strategy 3: Try script directory (if __file__ exists)
        if not vocabular_imported:
            try:
                script_path = os.path.dirname(os.path.abspath(__file__))
                if script_path not in sys.path:
                    sys.path.insert(0, script_path)
                from vocabular import map_and_rename_columns
                vocabular_imported = True
                log_status(f"✓ Successfully imported vocabular from script directory", "info")
            except (NameError, ImportError):
                pass
        
        # If still not imported, provide detailed error
        if not vocabular_imported:
            error_msg = "❌ Error: Could not import vocabular module"
            log_status(error_msg, "error")
            log_status(f"   Current working directory: {os.getcwd()}", "error")
            log_status(f"   Python path entries (first 5):", "error")
            for i, path in enumerate(sys.path[:5], 1):
                log_status(f"     {i}. {path}", "error")
            log_status(f"   Searched in:", "error")
            # Re-check paths for error message
            error_paths = [
                os.getcwd(),
                '/content/CANF-test-updated',
                '/content/CANF-test-updated/test folder',
                os.path.join(os.getcwd(), 'CANF-test-updated'),
                os.path.join(os.getcwd(), 'CANF-test-updated', 'test folder'),
            ]
            for path in error_paths:
                if path and os.path.exists(path):
                    vocab_file = os.path.join(path, 'vocabular.py')
                    exists = "✓" if os.path.exists(path) else "✗"
                    vocab_exists = "✓" if os.path.exists(vocab_file) else "✗"
                    log_status(f"     {exists} {path} (vocabular.py: {vocab_exists})", "error")
            log_status(f"   Please ensure vocabular.py is in one of these locations", "error")
            raise ImportError("Could not import vocabular module. Please ensure vocabular.py is accessible.")

        try:
            # Parse ignore_rate_card_columns from comma-separated string to list
            ignore_columns_list = None
            if ignore_rate_card_columns and ignore_rate_card_columns.strip():
                ignore_columns_list = [col.strip() for col in ignore_rate_card_columns.split(',') if col.strip()]
            
            # Convert header_row and end_column to integers if provided
            header_row_int = None
            end_column_int = None
            if origin_header_row is not None:
                try:
                    header_row_int = int(origin_header_row)
                except (ValueError, TypeError):
                    header_row_int = None
            if origin_end_column is not None:
                try:
                    end_column_int = int(origin_end_column)
                except (ValueError, TypeError):
                    end_column_int = None
            
            # Use filenames (relative to input/) for vocabular mapping
            log_status(f"\n{'='*80}", "info")
            log_status(f"Processing Vocabulary Mapping", "info")
            log_status(f"  - Rate Card: {rate_card_filename}", "info")
            log_status(f"  - ETOF: {etof_filename}", "info")
            log_status(f"  - Origin: {origin_filename if origin_filename else 'None'}", "info")
            log_status(f"  - LC: {len(lc_filenames)} file(s) - {', '.join(lc_filenames) if lc_filenames else 'None'}", "info")
            log_status(f"  - Order Files: {order_files_filename if order_files_filename else 'None'}", "info")
            log_status(f"  - Shipper ID: {shipper_id}", "info")
            log_status(f"  - Ignore columns: {ignore_columns_list if ignore_columns_list else 'None'}", "info")
            log_status(f"{'='*80}", "info")
            # Pass list of filenames if multiple, single filename if one, or None
            lc_input_param = lc_filenames if len(lc_filenames) > 1 else (lc_filenames[0] if lc_filenames else None)
            vocab_result = map_and_rename_columns(
                rate_card_file_path=rate_card_filename,
                etof_file_path=etof_filename,
                origin_file_path=origin_filename,
                origin_header_row=header_row_int,
                origin_end_column=end_column_int,
                order_files_path=order_files_filename,
                lc_input_path=lc_input_param,
                shipper_id=shipper_id,
                output_txt_path="column_mapping_results.txt",
                ignore_rate_card_columns=ignore_columns_list
            )
            etof_renamed, lc_renamed, origin_renamed = vocab_result
            log_status(f"\nVocabulary Mapping Results:", "info")
            if not etof_renamed.empty:
                log_status(f"  - ETOF renamed: {etof_renamed.shape[0]} rows x {etof_renamed.shape[1]} columns", "info")
                log_status(f"    First 3 rows:", "info")
                log_status(etof_renamed.head(3).to_string(), "info")
            if not lc_renamed.empty:
                log_status(f"  - LC renamed: {lc_renamed.shape[0]} rows x {lc_renamed.shape[1]} columns", "info")
                log_status(f"    First 3 rows:", "info")
                log_status(lc_renamed.head(3).to_string(), "info")
            if not origin_renamed.empty:
                log_status(f"  - Origin renamed: {origin_renamed.shape[0]} rows x {origin_renamed.shape[1]} columns", "info")
                log_status(f"    First 3 rows:", "info")
                log_status(origin_renamed.head(3).to_string(), "info")
            log_status(f"{'='*80}\n", "info")
        except Exception as e:
            log_status(f"⚠️ Warning: Vocabulary mapping failed: {e}", "warning")
            import traceback
            error_trace = traceback.format_exc()
            log_status(f"Traceback: {error_trace}", "error")
    finally:
        # Restore original working directory
        os.chdir(original_cwd)

    # --- MATCHING (matching.py) ---
    matching_file = None
    try:
        from matching import run_matching
        # Change back to script directory for matching (it expects to be in script_dir)
        os.chdir(script_dir)
        # Pass the rate card filename to run_matching
        log_status(f"\n{'='*80}", "info")
        log_status(f"Running Matching Process", "info")
        log_status(f"  - Rate Card file: {rate_card_filename}", "info")
        log_status(f"  - Current directory: {os.getcwd()}", "info")
        log_status(f"{'='*80}", "info")
        matching_file = run_matching(rate_card_file_path=rate_card_filename)
        if matching_file and not os.path.exists(matching_file):
            log_status(f"⚠️ Warning: Matching output file not found at: {matching_file}", "warning")
            matching_file = None
        elif matching_file:
            log_status(f"✓ Matching completed. Output file: {matching_file}", "info")
    except Exception as e:
        log_status(f"⚠️ Warning: Matching failed: {e}", "warning")
        import traceback
        error_trace = traceback.format_exc()
        log_status(f"Traceback: {error_trace}", "error")
    finally:
        # Restore original working directory after matching
        os.chdir(original_cwd)
    
    # Try to find the matching file in common locations
    if not matching_file:
        possible_locations = [
            "Matched_Shipments_with.xlsx",
            os.path.join(output_dir, "Matched_Shipments_with.xlsx")
        ]
        for loc in possible_locations:
            if os.path.exists(loc):
                matching_file = loc
                break

    # --- PIVOT CREATION ---
    # Only run pivot creation if matching file exists
    if matching_file and os.path.exists(matching_file):
        try:
            from pivot_creation import update_canf_file
            log_status(f"Running Pivot Creation...", "info")
            update_canf_file(matching_output_file=matching_file, shipper_value=shipper_id)
            log_status(f"✓ Pivot creation completed", "info")
        except Exception as e:
            log_status(f"⚠️ Warning: Pivot creation failed: {e}", "warning")
            import traceback
            error_trace = traceback.format_exc()
            log_status(f"Traceback: {error_trace}", "error")
    else:
        log_status("⚠️ Warning: Matching output file not found. Skipping pivot creation.", "warning")

    # --- Create Output File ---
    final_file_path = None
    
    # Try to find and copy the matching output file
    matching_output_found = False
    possible_matching_files = [
        matching_file,  # Use the file found earlier
        "Matched_Shipments_with.xlsx",
        os.path.join(output_dir, "Matched_Shipments_with.xlsx"),
        os.path.join(os.getcwd(), "Matched_Shipments_with.xlsx"),
        os.path.join(script_dir, "Matched_Shipments_with.xlsx")
    ]
    
    for matching_file_path in possible_matching_files:
        if matching_file_path and os.path.exists(matching_file_path):
            try:
                shutil.copyfile(matching_file_path, result_xlsx_path)
                final_file_path = result_xlsx_path
                matching_output_found = True
                log_status(f"✓ Output file created: {result_xlsx_path}", "info")
                break
            except Exception as e:
                log_status(f"⚠️ Warning: Could not copy matching file: {e}", "warning")
                continue
    
    # If no matching file found, create a summary/status file
    if not matching_output_found:
        try:
            import pandas as pd
            from datetime import datetime
            
            # Create a status summary Excel file
            status_data = {
                'Status': ['Workflow Completed'],
                'Timestamp': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                'Shipper ID': [shipper_id],
                'Rate Card File': [os.path.basename(rate_card_path) if rate_card_path else 'Not provided'],
                'ETOF File': [os.path.basename(etof_path) if etof_path else 'Not provided'],
                'LC File': [', '.join([os.path.basename(f) for f in lc_path]) if isinstance(lc_path, list) and lc_path else (os.path.basename(lc_path) if lc_path else 'Not provided')],
                'Origin File': [os.path.basename(origin_path) if origin_path else 'Not provided'],
                'Order Files': [os.path.basename(order_files_path) if order_files_path else 'Not provided'],
                'Matching Output': ['Not found - workflow may have failed or matching did not produce output']
            }
            
            status_df = pd.DataFrame(status_data)
            status_df.to_excel(result_xlsx_path, index=False, sheet_name='Workflow Status')
            final_file_path = result_xlsx_path
            log_status(f"⚠️ Status file created (matching output not found): {result_xlsx_path}", "warning")
        except Exception as e:
            error_msg = f"❌ Error creating status file: {e}"
            log_status(error_msg, "error")
            status_summary = ["❌ CRITICAL ERROR:", error_msg, "", "All status messages:", "-" * 80] + status_messages
            return None, "\n".join(status_summary)

    # Prepare status summary
    status_summary = []
    status_summary.append("=" * 80)
    status_summary.append("WORKFLOW EXECUTION SUMMARY")
    status_summary.append("=" * 80)
    status_summary.append("")
    
    if errors:
        status_summary.append(f"❌ ERRORS ({len(errors)}):")
        for i, error in enumerate(errors, 1):
            status_summary.append(f"  {i}. {error}")
        status_summary.append("")
    
    if warnings:
        status_summary.append(f"⚠️  WARNINGS ({len(warnings)}):")
        for i, warning in enumerate(warnings, 1):
            status_summary.append(f"  {i}. {warning}")
        status_summary.append("")
    
    if final_file_path and os.path.exists(final_file_path):
        status_summary.append(f"✅ SUCCESS: Output file created at: {final_file_path}")
        status_summary.append("")
        status_summary.append("All status messages:")
        status_summary.append("-" * 80)
        status_summary.extend(status_messages)
        status_text = "\n".join(status_summary)
        return final_file_path, status_text
    else:
        error_msg = "❌ Workflow did not complete. Check errors above for details."
        status_summary.append(error_msg)
        status_summary.append("")
        status_summary.append("All status messages:")
        status_summary.append("-" * 80)
        status_summary.extend(status_messages)
        status_text = "\n".join(status_summary)
        return None, status_text

# ---- Gradio UI definition for Google Colab ----
with gr.Blocks(title="ETOF/LC/Rate Card/Order Workflow", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# 📊 ETOF/LC/Rate Card/Order Workflow Processor")
    gr.Markdown("### Process and match shipment data with rate cards")
    
    with gr.Accordion("📖 Instructions & Information", open=False):
        gr.Markdown("""
        ## How to Use This Workflow
        
        ### Step 1: Upload Required Files
        - **Rate Card File** (Required): Excel file containing rate card data (.xlsx or .xls)
        - **ETOF File** (Required): Excel file containing ETOF shipment data (.xlsx or .xls)
        - **Shipper ID** (Required): Enter the shipper identifier (e.g., "dairb")
        
        ### Step 2: Upload Optional Files (if available)
        - **LC File(s)** (Optional): You can upload one or multiple LC XML/Excel files
          - Files starting with "LC" and ending with ".xml" will be automatically processed
          - Multiple files will be combined into a single dataset
        - **Origin File** (Optional): Excel/CSV/EDI file with origin data
          - If provided, you'll need to specify:
            - **Header Row**: Row number where column names are located (1-indexed, like Excel)
            - **End Column**: Last column to read (leave empty to read all columns)
        - **Order Files Export** (Optional): Excel/CSV file with order data
        
        ### Step 3: Configure Advanced Options (Optional)
        - **Ignore Rate Card Columns**: Enter comma-separated column names to exclude from processing
          - Example: `Column1, Column2, Column3`
        
        ### Step 4: Run Workflow
        - Click "Run Full Workflow" button
        - Wait for processing to complete
        - Check the Status/Errors section for any issues
        - Download the Result.xlsx file when ready
        
        ## Workflow Steps
        1. **File Processing**: Each uploaded file is processed and validated
        2. **Vocabulary Mapping**: Columns are mapped and renamed to standard names
        3. **Order-LC-ETOF Mapping**: Optional mapping between order files, LC, and ETOF data
        4. **Matching**: Shipments are matched with rate card entries
        5. **Discrepancy Detection**: Identifies and reports discrepancies
        6. **Pivot Creation**: Creates summary pivot table
        7. **Output Generation**: Creates final Excel file with all results
        
        ## Output File Contents
        - **Matched Shipments**: Sheet with all matched shipments and discrepancy comments
        - **Rate Card Reference**: Reference data from rate card
        - **Pivot Data**: Summary pivot table with carrier, cause, and amounts
        
        ## Troubleshooting
        - **Errors are shown in red** in the Status/Errors section
        - **Warnings are shown in yellow** - these may not prevent completion
        - Check that all required files are uploaded
        - Verify file formats are correct (.xlsx, .xls, .xml, .csv, .edi)
        - Ensure Origin file header row and end column are correct if provided
        """)
    
    gr.Markdown("---")
    gr.Markdown("### 📁 File Upload")
    gr.Markdown("**Required:** Rate Card File, ETOF File, and Shipper ID  |  **Optional:** LC File(s), Origin File, Order Files")
    with gr.Row():
        rate_card_input = gr.File(label="Rate Card File (.xlsx, .xls) *Required", file_types=[".xlsx", ".xls"])
        etof_input = gr.File(label="ETOF File (.xlsx, .xls) *Required", file_types=[".xlsx", ".xls"])
        lc_input = gr.File(label="LC File(s) (.xlsx, .xls, .xml) *Optional", file_types=[".xlsx", ".xls", ".xml"], file_count="multiple")
    with gr.Row():
        origin_input = gr.File(label="Origin File (.xlsx, .xls, .csv, .edi) *Optional", file_types=[".xlsx", ".xls", ".csv", ".edi"])
        order_files_input = gr.File(label="Order Files Export (.xlsx, .xls, .csv) *Optional", file_types=[".xlsx", ".xls", ".csv"])
        shipper_id_input = gr.Textbox(label="Shipper ID *Required", placeholder="e.g. dairb or use ID as string")
    
    # Origin file parameters (shown conditionally)
    with gr.Row(visible=False) as origin_params_row:
        origin_header_row_input = gr.Number(
            label="Origin File Header Row (1-indexed, like Excel)",
            value=1,
            info="Row number where column headers are located (e.g., 15 for row 15). Required for CSV/Excel files, not needed for .edi files.",
            precision=0,
            minimum=1
        )
        origin_end_column_input = gr.Number(
            label="Origin File End Column (1-indexed, like Excel)",
            value=None,
            info="Last column to read (e.g., 33 for columns A through 33). Leave empty to read all columns.",
            precision=0,
            minimum=1
        )
    
    # Ignore rate card columns input
    ignore_rate_card_columns_input = gr.Textbox(
        label="Ignore Rate Card Columns (Optional)",
        placeholder="Enter column names separated by commas (e.g., Column1, Column2, Column3)",
        info="Rate card columns to exclude from processing. Separate multiple columns with commas."
    )
    
    gr.Markdown("---")
    launch_button = gr.Button("🚀 Run Full Workflow", variant="primary", size="lg")
    
    with gr.Row():
        out = gr.File(label="📥 Result.xlsx (Download Final Output)")
        status_output = gr.Textbox(
            label="📋 Status & Errors",
            lines=20,
            max_lines=30,
            interactive=False,
            placeholder="Workflow status and error messages will appear here...",
            show_copy_button=True
        )
    
    # Function to toggle visibility of origin parameters
    def toggle_origin_params(origin_file):
        return gr.update(visible=origin_file is not None)
    
    # Update origin parameters visibility when origin file changes
    origin_input.change(
        fn=toggle_origin_params,
        inputs=[origin_input],
        outputs=[origin_params_row]
    )

    def launch_workflow(rate_card_file, etof_file, lc_file, origin_file, order_files, shipper_id,
                       origin_header_row, origin_end_column, ignore_rate_card_columns):
        try:
            result_file, status_text = run_full_workflow_gradio(
                rate_card_file, etof_file, lc_file, origin_file, order_files, shipper_id,
                origin_header_row=origin_header_row,
                origin_end_column=origin_end_column,
                ignore_rate_card_columns=ignore_rate_card_columns
            )
            return result_file, status_text
        except Exception as e:
            import traceback
            error_details = f"❌ CRITICAL ERROR:\n{str(e)}\n\nTraceback:\n{traceback.format_exc()}"
            return None, error_details

    launch_button.click(
        launch_workflow,
        inputs=[
            rate_card_input, etof_input, lc_input, origin_input, order_files_input, shipper_id_input,
            origin_header_row_input, origin_end_column_input, ignore_rate_card_columns_input
        ],
        outputs=[out, status_output]
    )

if __name__ == "__main__":
    import sys
    
    # Create input and output folders when program starts
    # Handle Colab environment where __file__ is not defined
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # In Colab or interactive environments, use current working directory
        script_dir = os.getcwd()
    
    input_dir = os.path.join(script_dir, "input")
    output_dir = os.path.join(script_dir, "output")
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 Created input folder: {input_dir}")
    print(f"📁 Created output folder: {output_dir}")
    
    # Check if running in Colab
    in_colab = 'google.colab' in sys.modules
    
    if in_colab:
        use_share = False  # Change to True if you prefer public URL
        
        if use_share:
            print("🚀 Launching Gradio interface for Google Colab (public URL)...")
            demo.launch(share=True, debug=False, show_error=True)
        else:
            print("🚀 Launching Gradio interface for Google Colab (local access)...")
            demo.launch(share=False, server_name="0.0.0.0", debug=False, show_error=True)
    else:
        # For local execution
        print("🚀 Launching Gradio interface locally...")
        print(f"💡 Input files will be saved to: {input_dir}")
        print(f"💡 Output files will be saved to: {output_dir}")
        demo.launch(server_name="127.0.0.1", share=False)
