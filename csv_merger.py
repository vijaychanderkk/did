import pandas as pd
import os
from io import StringIO
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential, AzureCliCredential, ManagedIdentityCredential
from typing import List, Optional

def merge_csv_files_by_pattern(
    container_name: str,
    storage_account_name: str = None,
    account_url: str = None,
    connection_string: str = None,
    credential: str = None,
    use_azure_ad: bool = True,
    base_path: str = "",
    subfolders: Optional[List[str]] = None,
    include_root: bool = True,
    truth_output: str = "merged_truth.csv",
    other_output: str = "merged_other.csv",
    add_metadata: bool = False,
    truth_pattern: str = "truth"
):
    """
    Merge CSV files from Azure Blob Storage based on filename patterns.
    
    Args:
        container_name (str): Name of the Azure storage container
        storage_account_name (str): Azure storage account name (for Azure AD auth)
        account_url (str): Full Azure storage account URL
        connection_string (str): Azure storage connection string (alternative auth method)
        credential (str): Azure credential (SAS token, account key, etc.)
        use_azure_ad (bool): Whether to use Azure AD authentication (recommended)
        base_path (str): Base folder path within container (e.g., "output" or "output/")
        subfolders (list): List of subfolder names under base_path to process. If None, processes all subfolders under base_path
        include_root (bool): Whether to include files directly in base_path root
        truth_output (str): Output filename for merged truth files
        other_output (str): Output filename for merged other files
        add_metadata (bool): Whether to add source_file and source_path columns
        truth_pattern (str): Pattern to identify truth files (case insensitive)
    """
    
    # Initialize blob service client
    if use_azure_ad:
        # Use Azure AD authentication (recommended for Storage Blob Data Reader role)
        if not account_url and storage_account_name:
            account_url = f"https://{storage_account_name}.blob.core.windows.net"
        elif not account_url:
            raise ValueError("Either account_url or storage_account_name must be provided for Azure AD auth")
        
        # Try different Azure AD credential types
        try:
            # DefaultAzureCredential tries multiple auth methods in order:
            # 1. Environment variables
            # 2. Managed identity
            # 3. Azure CLI
            # 4. Visual Studio Code
            # 5. Azure PowerShell
            azure_credential = DefaultAzureCredential()
            blob_service_client = BlobServiceClient(account_url=account_url, credential=azure_credential)
            print("✅ Using Azure AD authentication (DefaultAzureCredential)")
        except Exception as e:
            print(f"Azure AD auth failed: {e}")
            print("Make sure you're logged in with 'az login' or running in an environment with managed identity")
            return
    elif connection_string:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        print("✅ Using connection string authentication")
    elif account_url and credential:
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        print("✅ Using SAS token/account key authentication")
    else:
        raise ValueError("Authentication method required: use_azure_ad=True with storage_account_name, or provide connection_string, or (account_url + credential)")
    
    # Get container client
    container_client = blob_service_client.get_container_client(container_name)
    
    # Normalize base_path
    if base_path:
        base_path = base_path.strip('/')
        if base_path:
            base_path = base_path + '/'
        print(f"Focusing on base path: '{base_path}' in container '{container_name}'")
    else:
        print(f"Processing entire container '{container_name}'")
    
    # List blobs with prefix filter
    try:
        if base_path:
            all_blobs = list(container_client.list_blobs(name_starts_with=base_path))
        else:
            all_blobs = list(container_client.list_blobs())
        
        csv_blobs = [blob for blob in all_blobs if blob.name.lower().endswith('.csv')]
        print(f"Found {len(csv_blobs)} CSV files in specified path")
    except Exception as e:
        print(f"Error accessing container: {e}")
        return
    
    # Filter blobs based on subfolders within base_path
    filtered_blobs = []
    
    if subfolders is None:
        # Process all CSV files under base_path
        filtered_blobs = csv_blobs
    else:
        # Process files in specific subfolders under base_path
        for blob in csv_blobs:
            blob_path = blob.name
            
            # Remove base_path prefix to get relative path
            if base_path and blob_path.startswith(base_path):
                relative_path = blob_path[len(base_path):]
            else:
                relative_path = blob_path
            
            # Check if file is in root of base_path (no additional '/' in relative path)
            if include_root and '/' not in relative_path:
                filtered_blobs.append(blob)
                continue
            
            # Check if file is in any of the specified subfolders under base_path
            for subfolder in subfolders:
                subfolder_normalized = subfolder.strip('/')
                if relative_path.startswith(subfolder_normalized + '/'):
                    filtered_blobs.append(blob)
                    break
    
    print(f"Processing {len(filtered_blobs)} CSV files after filtering")
    
    if not filtered_blobs:
        print("No CSV files found matching the criteria")
        return
    
    # Separate files based on truth pattern
    truth_blobs = []
    other_blobs = []
    
    for blob in filtered_blobs:
        filename = os.path.basename(blob.name).lower()
        if truth_pattern.lower() in filename:
            truth_blobs.append(blob)
        else:
            other_blobs.append(blob)
    
    print(f"Truth files: {len(truth_blobs)}")
    print(f"Other files: {len(other_blobs)}")
    
    # Function to merge blobs
    def merge_blobs(blob_list, output_filename):
        if not blob_list:
            print(f"No files to merge for {output_filename}")
            return
        
        merged_data = []
        
        for blob in blob_list:
            try:
                # Download blob content
                blob_client = container_client.get_blob_client(blob.name)
                blob_content = blob_client.download_blob().readall().decode('utf-8')
                
                # Read CSV from string
                df = pd.read_csv(StringIO(blob_content))
                
                # Filter out metadata columns if they exist (from previous merges)
                metadata_columns = ['source_file', 'source_path', 'file_size', 'merge_timestamp', 'container_name']
                df = df.drop(columns=[col for col in metadata_columns if col in df.columns])
                
                # Add metadata columns only if requested
                if add_metadata:
                    df['source_file'] = os.path.basename(blob.name)
                    df['source_path'] = blob.name
                    df['container_name'] = container_name
                
                merged_data.append(df)
                print(f"  Added {blob.name} ({len(df)} rows)")
                
            except Exception as e:
                print(f"  Error reading {blob.name}: {e}")
        
        if merged_data:
            # Concatenate all dataframes
            final_df = pd.concat(merged_data, ignore_index=True)
            
            # Save merged file locally
            final_df.to_csv(output_filename, index=False)
            
            print(f"✅ Merged {len(blob_list)} files into {output_filename}")
            print(f"   Total rows: {len(final_df)}")
            print(f"   Columns: {list(final_df.columns)}")
            print()
            
            return final_df
    
    # Show files to be processed
    if truth_blobs:
        print("\n🔍 Truth files to merge:")
        for blob in truth_blobs:
            print(f"  - {blob.name}")
        merge_blobs(truth_blobs, truth_output)
    
    if other_blobs:
        print("📄 Other files to merge:")
        for blob in other_blobs:
            print(f"  - {blob.name}")
        merge_blobs(other_blobs, other_output)

def merge_csv_with_azure_ad(
    container_name: str,
    storage_account_name: str,
    base_path: str = "",
    subfolders: Optional[List[str]] = None,
    include_root: bool = True,
    **kwargs
):
    """
    Convenience function for Azure AD authentication with Storage Blob Data Reader role.
    
    Args:
        container_name (str): Name of the Azure storage container
        storage_account_name (str): Azure storage account name
        base_path (str): Base folder path within container (e.g., "output")
        subfolders (list): List of subfolder names under base_path to process
        include_root (bool): Whether to include files directly in base_path root
        **kwargs: Additional arguments passed to merge_csv_files_by_pattern
    """
    
    merge_csv_files_by_pattern(
        container_name=container_name,
        storage_account_name=storage_account_name,
        use_azure_ad=True,
        base_path=base_path,
        subfolders=subfolders,
        include_root=include_root,
        **kwargs
    )

def merge_csv_from_url(
    azure_url: str,
    subfolders: Optional[List[str]] = None,
    include_root: bool = True,
    **kwargs
):
    """
    Convenience function to parse Azure URL and merge CSVs.
    
    Args:
        azure_url (str): Full Azure blob URL like "https://storage.blob.core.windows.net/container/path/"
        subfolders (list): List of subfolder names to process
        include_root (bool): Whether to include files directly in the path
        **kwargs: Additional arguments passed to merge_csv_files_by_pattern
    
    Example:
        merge_csv_from_url(
            "https://storage.blob.core.windows.net/deidlob/output/",
            subfolders=["exp1", "exp2"]
        )
    """
    import urllib.parse
    
    # Parse the URL
    parsed = urllib.parse.urlparse(azure_url)
    
    # Extract storage account name from hostname
    hostname_parts = parsed.hostname.split('.')
    storage_account_name = hostname_parts[0]
    
    # Extract container and path from URL path
    path_parts = parsed.path.strip('/').split('/', 1)
    container_name = path_parts[0]
    base_path = path_parts[1] if len(path_parts) > 1 else ""
    
    print(f"Parsed URL:")
    print(f"  Storage Account: {storage_account_name}")
    print(f"  Container: {container_name}")
    print(f"  Base Path: '{base_path}'")
    
    merge_csv_files_by_pattern(
        container_name=container_name,
        storage_account_name=storage_account_name,
        use_azure_ad=True,
        base_path=base_path,
        subfolders=subfolders,
        include_root=include_root,
        **kwargs
    )

if __name__ == "__main__":
    # Example 1: Using your specific Azure URL
    print("=== CSV Merger from Azure URL ===")
    
    # Your Azure URL
    AZURE_URL = "https://storage.blob.core.windows.net/deidlob/output/"
    
    merge_csv_from_url(
        azure_url=AZURE_URL,
        subfolders=["exp1", "exp2", "results"],  # Process these subfolders under /output/
        include_root=True,  # Also include files directly in /output/
        add_metadata=False,
        truth_pattern="truth"
    )
    
    print("\n" + "="*50)
    
    # Example 2: Process ALL subfolders under /output/
    print("=== Process All Subfolders in Output ===")
    
    merge_csv_from_url(
        azure_url="https://storage.blob.core.windows.net/deidlob/output/",
        subfolders=None,  # Process ALL subfolders under /output/
        include_root=True,
        add_metadata=True,
        truth_output="all_truth_from_output.csv",
        other_output="all_other_from_output.csv"
    )
    
    print("\n" + "="*50)
    
    # Example 3: Manual specification (alternative approach)
    print("=== Manual Container and Path Specification ===")
    
    merge_csv_with_azure_ad(
        container_name="deidlob",
        storage_account_name="storage",  # From your URL
        base_path="output",  # Focus on the 'output' folder
        subfolders=["subfolder1", "subfolder2"],  # Specific subfolders under output/
        include_root=False,  # Don't include files directly in output/ root
        add_metadata=False
    )
    
    print("\n" + "="*50)
    
    # Example 4: Just files directly in /output/ (no subfolders)
    print("=== Only Files in Output Root ===")
    
    merge_csv_from_url(
        azure_url="https://storage.blob.core.windows.net/deidlob/output/",
        subfolders=[],  # Empty list = no subfolders
        include_root=True,  # Only files directly in /output/
        truth_output="output_root_truth.csv",
        other_output="output_root_other.csv"
    )
