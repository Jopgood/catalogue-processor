import pandas as pd
import json
import csv
from pathlib import Path
import logging
from typing import Dict, List, Any, Optional, Union, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ManifestHandler:
    """Handle manifest files containing information about audio files.
    
    This class is responsible for loading, parsing, and querying manifest files
    that contain information about audio files in a bucket or storage system.
    """
    
    def __init__(self, manifest_path: str):
        """Initialize the manifest handler.
        
        Args:
            manifest_path: Path to the manifest file
        """
        self.manifest_path = manifest_path
        self.manifest_data = None
        self.file_index = {}  # For fast lookup by filename
        
    def load_manifest(self) -> Any:
        """Load the manifest file into memory.
        
        Returns:
            The loaded manifest data (format depends on the file type)
        """
        path = Path(self.manifest_path)
        logger.info(f"Loading manifest from {self.manifest_path}")
        
        try:
            if path.suffix.lower() == '.csv':
                self.manifest_data = pd.read_csv(self.manifest_path)
                logger.info(f"Loaded CSV manifest with {len(self.manifest_data)} entries")
                
            elif path.suffix.lower() == '.json':
                with open(self.manifest_path, 'r') as f:
                    self.manifest_data = json.load(f)
                if isinstance(self.manifest_data, dict):
                    logger.info(f"Loaded JSON manifest with {len(self.manifest_data)} entries")
                else:
                    logger.info(f"Loaded JSON manifest with {len(self.manifest_data)} entries")
                    
            elif path.suffix.lower() in ['.xls', '.xlsx']:
                self.manifest_data = pd.read_excel(self.manifest_path)
                logger.info(f"Loaded Excel manifest with {len(self.manifest_data)} entries")
                
            else:
                raise ValueError(f"Unsupported manifest file format: {path.suffix}")
                
            # Build the file index for faster lookups
            self._build_file_index()
            
            return self.manifest_data
            
        except Exception as e:
            logger.error(f"Error loading manifest: {str(e)}")
            raise
    
    def _build_file_index(self):
        """Build an index for fast lookup of files by filename."""
        logger.info("Building file index for fast lookups")
        
        if isinstance(self.manifest_data, pd.DataFrame):
            # For pandas DataFrame
            path_col = self._find_path_column()
            if not path_col:
                logger.warning("Could not find path column in manifest DataFrame")
                return
                
            for idx, row in self.manifest_data.iterrows():
                filepath = row[path_col]
                filename = Path(filepath).name
                self.file_index[filename] = idx
                
        elif isinstance(self.manifest_data, list):
            # For list of dictionaries
            path_key = self._find_path_key_in_dict(self.manifest_data[0] if self.manifest_data else {})
            if not path_key:
                logger.warning("Could not find path key in manifest list items")
                return
                
            for idx, item in enumerate(self.manifest_data):
                filepath = item.get(path_key, "")
                filename = Path(filepath).name
                self.file_index[filename] = idx
                
        elif isinstance(self.manifest_data, dict):
            # For dictionary of items
            # First, find a sample item to determine structure
            sample_key = next(iter(self.manifest_data))
            sample_item = self.manifest_data[sample_key]
            
            if isinstance(sample_item, dict):
                path_key = self._find_path_key_in_dict(sample_item)
                if not path_key:
                    logger.warning("Could not find path key in manifest dictionary items")
                    return
                    
                for key, item in self.manifest_data.items():
                    filepath = item.get(path_key, "")
                    filename = Path(filepath).name
                    self.file_index[filename] = key
            else:
                # If the values aren't dictionaries, we can't easily build an index
                logger.warning("Manifest dictionary does not contain structured items, skipping index")
        
        logger.info(f"Built file index with {len(self.file_index)} entries")
    
    def _find_path_column(self) -> Optional[str]:
        """Find the column in a DataFrame that likely contains file paths."""
        if not isinstance(self.manifest_data, pd.DataFrame):
            return None
            
        # Look for common column names that might contain paths
        for col_name in ['path', 'filepath', 'file_path', 'location', 'uri', 'url']:
            if col_name in self.manifest_data.columns:
                return col_name
                
        # If no obvious column name, look for columns that have string values containing '/'
        for col in self.manifest_data.columns:
            if self.manifest_data[col].dtype == 'object':  # String columns are 'object' type
                sample = self.manifest_data[col].dropna().iloc[0] if not self.manifest_data[col].dropna().empty else ""
                if isinstance(sample, str) and ('/' in sample or '\\' in sample):
                    return col
                    
        return None
    
    def _find_path_key_in_dict(self, sample_dict: Dict) -> Optional[str]:
        """Find the key in a dictionary that likely contains a file path."""
        if not sample_dict:
            return None
            
        # Look for common key names that might contain paths
        for key_name in ['path', 'filepath', 'file_path', 'location', 'uri', 'url']:
            if key_name in sample_dict:
                return key_name
                
        # If no obvious key name, look for keys that have string values containing '/'
        for key, value in sample_dict.items():
            if isinstance(value, str) and ('/' in value or '\\' in value):
                return key
                
        return None
    
    def find_audio_file(self, audio_filename: str) -> Optional[Dict[str, Any]]:
        """Find an audio file in the manifest by filename.
        
        Args:
            audio_filename: The filename to look for
            
        Returns:
            Dictionary containing manifest entry for the file, or None if not found
        """
        # Convert to just the filename if a path was provided
        filename = Path(audio_filename).name
        
        # Try direct lookup in the index first (fastest)
        if filename in self.file_index:
            idx = self.file_index[filename]
            
            if isinstance(self.manifest_data, pd.DataFrame):
                return self.manifest_data.iloc[idx].to_dict()
            elif isinstance(self.manifest_data, list):
                return self.manifest_data[idx]
            elif isinstance(self.manifest_data, dict):
                return self.manifest_data[idx]
        
        # If that fails, try a more flexible search
        logger.debug(f"File {filename} not found in index, trying flexible search")
        
        if isinstance(self.manifest_data, pd.DataFrame):
            path_col = self._find_path_column()
            if path_col:
                matches = self.manifest_data[self.manifest_data[path_col].str.contains(filename, na=False)]
                if not matches.empty:
                    return matches.iloc[0].to_dict()
                    
        elif isinstance(self.manifest_data, list):
            path_key = self._find_path_key_in_dict(self.manifest_data[0] if self.manifest_data else {})
            if path_key:
                for item in self.manifest_data:
                    if filename in item.get(path_key, ""):
                        return item
                        
        elif isinstance(self.manifest_data, dict):
            sample_key = next(iter(self.manifest_data))
            sample_item = self.manifest_data[sample_key]
            
            if isinstance(sample_item, dict):
                path_key = self._find_path_key_in_dict(sample_item)
                if path_key:
                    for key, item in self.manifest_data.items():
                        if filename in item.get(path_key, ""):
                            return {**item, 'id': key}
        
        logger.warning(f"Could not find file {filename} in manifest")
        return None
    
    def update_manifest_with_metadata(self, metadata_list: List[Dict[str, Any]]) -> Any:
        """Update the manifest data with extracted metadata.
        
        Args:
            metadata_list: List of dictionaries containing extracted metadata
            
        Returns:
            Updated manifest data
        """
        logger.info(f"Updating manifest with metadata for {len(metadata_list)} files")
        
        if isinstance(self.manifest_data, pd.DataFrame):
            # Create a copy to avoid modifying the original
            updated_manifest = self.manifest_data.copy()
            
            # Add new columns if they don't exist
            for col in ['isrc', 'track_title', 'artist']:
                if col not in updated_manifest.columns:
                    updated_manifest[col] = None
            
            # Update rows with metadata
            for metadata in metadata_list:
                filename = Path(metadata['audio_filename']).name
                if filename in self.file_index:
                    idx = self.file_index[filename]
                    updated_manifest.loc[idx, 'isrc'] = metadata.get('isrc')
                    updated_manifest.loc[idx, 'track_title'] = metadata.get('track_title')
                    updated_manifest.loc[idx, 'artist'] = metadata.get('artist')
            
            self.manifest_data = updated_manifest
            
        elif isinstance(self.manifest_data, list):
            for metadata in metadata_list:
                filename = Path(metadata['audio_filename']).name
                if filename in self.file_index:
                    idx = self.file_index[filename]
                    self.manifest_data[idx]['isrc'] = metadata.get('isrc')
                    self.manifest_data[idx]['track_title'] = metadata.get('track_title')
                    self.manifest_data[idx]['artist'] = metadata.get('artist')
                    
        elif isinstance(self.manifest_data, dict):
            for metadata in metadata_list:
                filename = Path(metadata['audio_filename']).name
                if filename in self.file_index:
                    key = self.file_index[filename]
                    self.manifest_data[key]['isrc'] = metadata.get('isrc')
                    self.manifest_data[key]['track_title'] = metadata.get('track_title')
                    self.manifest_data[key]['artist'] = metadata.get('artist')
        
        logger.info("Manifest update complete")
        return self.manifest_data
    
    def save_updated_manifest(self, output_path: str) -> str:
        """Save the updated manifest to a file.
        
        Args:
            output_path: Path where to save the updated manifest
            
        Returns:
            Path to the saved file
        """
        output_path = Path(output_path)
        logger.info(f"Saving updated manifest to {output_path}")
        
        try:
            if output_path.suffix.lower() == '.csv':
                if isinstance(self.manifest_data, pd.DataFrame):
                    self.manifest_data.to_csv(output_path, index=False)
                else:
                    pd.DataFrame(self.manifest_data).to_csv(output_path, index=False)
                    
            elif output_path.suffix.lower() == '.json':
                with open(output_path, 'w') as f:
                    json.dump(self.manifest_data, f, indent=2)
                    
            elif output_path.suffix.lower() in ['.xls', '.xlsx']:
                if isinstance(self.manifest_data, pd.DataFrame):
                    self.manifest_data.to_excel(output_path, index=False)
                else:
                    pd.DataFrame(self.manifest_data).to_excel(output_path, index=False)
                    
            else:
                raise ValueError(f"Unsupported output file format: {output_path.suffix}")
                
            logger.info(f"Successfully saved updated manifest to {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error saving updated manifest: {str(e)}")
            raise