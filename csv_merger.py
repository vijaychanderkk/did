import pandas as pd
import glob
import os

def merge_csv_files_by_pattern(directory_path=".", truth_output="merged_truth.csv", other_output="merged_other.csv"):
    """
    Merge CSV files based on filename patterns, searching recursively in subfolders.
    
    Args:
        directory_path (str): Path to directory containing CSV files
        truth_output (str): Output filename for merged truth files
        other_output (str): Output filename for merged other files
    """
    
    # Get all CSV files recursively in the directory and subfolders
    csv_pattern = os.path.join(directory_path, "**", "*.csv")
    csv_files = glob.glob(csv_pattern, recursive=True)
    
    if not csv_files:
        print(f"No CSV files found in {directory_path}")
        return
    
    print(f"Found {len(csv_files)} CSV files")
    
    # Separate files based on pattern
    truth_files = []
    other_files = []
    
    for file in csv_files:
        filename = os.path.basename(file).lower()
        if "truth" in filename:
            truth_files.append(file)
        else:
            other_files.append(file)
    
    print(f"Truth files: {len(truth_files)}")
    print(f"Other files: {len(other_files)}")
    
    # Function to merge files and add source column
    def merge_files(file_list, output_filename):
        if not file_list:
            print(f"No files to merge for {output_filename}")
            return
        
        merged_data = []
        
        for file in file_list:
            try:
                # Read CSV file
                df = pd.read_csv(file)
                
                # Add source filename and relative path
                df['source_file'] = os.path.basename(file)
                df['source_path'] = os.path.relpath(file, directory_path)
                
                merged_data.append(df)
                print(f"  Added {os.path.relpath(file, directory_path)} ({len(df)} rows)")
                
            except Exception as e:
                print(f"  Error reading {file}: {e}")
        
        if merged_data:
            # Concatenate all dataframes
            final_df = pd.concat(merged_data, ignore_index=True)
            
            # Save merged file
            output_path = os.path.join(directory_path, output_filename)
            final_df.to_csv(output_path, index=False)
            
            print(f"✅ Merged {len(file_list)} files into {output_filename}")
            print(f"   Total rows: {len(final_df)}")
            print(f"   Columns: {list(final_df.columns)}")
            print()
    
    # Merge truth files
    if truth_files:
        print("\n🔍 Merging truth files:")
        for file in truth_files:
            rel_path = os.path.relpath(file, directory_path)
            print(f"  - {rel_path}")
        merge_files(truth_files, truth_output)
    
    # Merge other files
    if other_files:
        print("📄 Merging other files:")
        for file in other_files:
            rel_path = os.path.relpath(file, directory_path)
            print(f"  - {rel_path}")
        merge_files(other_files, other_output)

def merge_csv_advanced(directory_path=".", truth_pattern="truth", case_sensitive=False):
    """
    Advanced version with more customization options.
    
    Args:
        directory_path (str): Path to directory containing CSV files
        truth_pattern (str): Pattern to identify truth files
        case_sensitive (bool): Whether pattern matching is case sensitive
    """
    
    csv_files = glob.glob(os.path.join(directory_path, "**", "*.csv"), recursive=True)
    
    if not csv_files:
        print(f"No CSV files found in {directory_path}")
        return
    
    truth_files = []
    other_files = []
    
    for file in csv_files:
        filename = os.path.basename(file)
        search_text = filename if case_sensitive else filename.lower()
        pattern = truth_pattern if case_sensitive else truth_pattern.lower()
        
        if pattern in search_text:
            truth_files.append(file)
        else:
            other_files.append(file)
    
    # Create output filenames with timestamp
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    truth_output = f"merged_{truth_pattern}_{timestamp}.csv"
    other_output = f"merged_other_{timestamp}.csv"
    
    # Merge files
    def merge_with_metadata(file_list, output_filename):
        if not file_list:
            return
        
        merged_data = []
        
        for file in file_list:
            try:
                df = pd.read_csv(file)
                
                # Add metadata columns
                df['source_file'] = os.path.basename(file)
                df['file_size'] = os.path.getsize(file)
                df['merge_timestamp'] = datetime.now().isoformat()
                
                merged_data.append(df)
                
            except Exception as e:
                print(f"Error reading {file}: {e}")
        
        if merged_data:
            final_df = pd.concat(merged_data, ignore_index=True)
            output_path = os.path.join(directory_path, output_filename)
            final_df.to_csv(output_path, index=False)
            
            print(f"✅ Created {output_filename} with {len(final_df)} rows")
    
    merge_with_metadata(truth_files, truth_output)
    merge_with_metadata(other_files, other_output)

if __name__ == "__main__":
    # Basic usage - merge files in current directory and all subfolders
    print("=== CSV Merger with Subfolder Support ===")
    merge_csv_files_by_pattern()
    
    # Example with custom directory
    # merge_csv_files_by_pattern(
    #     directory_path="./data",
    #     truth_output="all_truth_data.csv",
    #     other_output="all_regular_data.csv"
    # )
