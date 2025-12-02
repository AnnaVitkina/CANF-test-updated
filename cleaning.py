import os
import shutil

def clean_folder(folder_path):
    """
    Deletes all files and subfolders in the specified folder.
    Does not delete the folder itself.
    Returns a list of deleted items.
    """
    deleted_items = []
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                    deleted_items.append(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    deleted_items.append(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
    return deleted_items

def clean_input_and_output_folders():
    """
    Cleans both the 'input' and 'output' folders in the current directory.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_folder = os.path.join(script_dir, "input")
    output_folder = os.path.join(script_dir, "output")

    deleted_input = clean_folder(input_folder)
    deleted_output = clean_folder(output_folder)

    print(f"Deleted from input: {deleted_input}")
    print(f"Deleted from output: {deleted_output}")

# Example usage:
clean_input_and_output_folders()
