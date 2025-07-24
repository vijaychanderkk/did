import pandas as pd
import glob
import os

def merge_csv_files_by_pattern(directory_path=".", subfolders=None, truth_output="merged_truth.csv", other_output="merged_other.csv", add_metadata=False):
    """
    Merge CSV files based on filename patterns, searching in specific subfolders.
    
    Args:
        directory_path (str): Path to directory containing CSV files
        subfolders (list): List of subfolder names to process. If None, processes all subfolders
        truth_output (str): Output filename for merged truth files
        other_output (str): Output filename for merged other files
        add_metadata (bool): Whether to add source_file and source_path columns
    """
    
    # Get CSV files from specific subfolders or all subfolders
    csv_files = []
    
    if subfolders is None:
        # Process all subfolders (original behavior)
        csv_pattern = os.path.join(directory_path, "**", "*.csv")
        csv_files = glob.glob(csv_pattern, recursive=True)
    else:
        # Process only specified subfolders
        for subfolder in subfolders:
            subfolder_path = os.path.join(directory_path, subfolder)
            if os.path.exists(subfolder_path):
                csv_pattern = os.path.join(subfolder_path, "**", "*.csv")
                folder_files = glob.glob(csv_pattern, recursive=True)
                csv_files.extend(folder_files)
                print(f"Found {len(folder_files)} CSV files in '{subfolder}' folder")
            else:
                print(f"Warning: Subfolder '{subfolder}' not found in {directory_path}")
    
    if not csv_files:
        print(f"No CSV files found in specified locations")
        return
    
    print(f"Total CSV files found: {len(csv_files)}")
    
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
    
    # Function to merge files
    def merge_files(file_list, output_filename):
        if not file_list:
            print(f"No files to merge for {output_filename}")
            return
        
        merged_data = []
        
        for file in file_list:
            try:
                # Read CSV file
                df = pd.read_csv(file)
                
                # Filter out metadata columns if they exist (from previous merges)
                metadata_columns = ['source_file', 'source_path', 'file_size', 'merge_timestamp']
                df = df.drop(columns=[col for col in metadata_columns if col in df.columns])
                
                # Add metadata columns only if requested
                if add_metadata:
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

def merge_csv_advanced(directory_path=".", subfolders=None, truth_pattern="truth", case_sensitive=False, add_metadata=True):
    """
    Advanced version with more customization options and subfolder selection.
    
    Args:
        directory_path (str): Path to directory containing CSV files
        subfolders (list): List of subfolder names to process
        truth_pattern (str): Pattern to identify truth files
        case_sensitive (bool): Whether pattern matching is case sensitive
        add_metadata (bool): Whether to add metadata columns
    """
    
    # Get CSV files from specific subfolders
    csv_files = []
    
    if subfolders is None:
        csv_files = glob.glob(os.path.join(directory_path, "**", "*.csv"), recursive=True)
    else:
        for subfolder in subfolders:
            subfolder_path = os.path.join(directory_path, subfolder)
            if os.path.exists(subfolder_path):
                csv_pattern = os.path.join(subfolder_path, "**", "*.csv")
                folder_files = glob.glob(csv_pattern, recursive=True)
                csv_files.extend(folder_files)
    
    if not csv_files:
        print(f"No CSV files found in specified locations")
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
                
                # Filter out existing metadata columns
                metadata_columns = ['source_file', 'source_path', 'file_size', 'merge_timestamp']
                df = df.drop(columns=[col for col in metadata_columns if col in df.columns])
                
                # Add metadata columns only if requested
                if add_metadata:
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
    # Example 1: Process only specific subfolders without metadata
    print("=== CSV Merger with Specific Folders ===")
    merge_csv_files_by_pattern(
        directory_path=".",
        subfolders=["data", "results", "backup"],  # Only process these folders
        add_metadata=False  # Don't add source columns
    )
    
    print("\n" + "="*50)
    
    # Example 2: Process all subfolders with metadata
    print("=== CSV Merger for All Folders with Metadata ===")
    merge_csv_files_by_pattern(
        directory_path=".",
        subfolders=None,  # Process all subfolders
        add_metadata=True  # Add source columns
    )
    
    print("\n" + "="*50)
    
    # Example 3: Advanced merge with specific folders
    print("=== Advanced CSV Merger ===")
    merge_csv_advanced(
        directory_path="./my_data",
        subfolders=["experiment1", "experiment2"],
        truth_pattern="ground_truth",
        add_metadata=False
    )
