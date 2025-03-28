import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
import os
import logging
from pathlib import Path
from tqdm import tqdm
from typing import List, Dict, Optional, Any, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class XMLProcessor:
    """Process XML files to extract audio metadata.
    
    This class handles finding and processing XML files to extract metadata
    about audio files, such as ISRC, track title, and artist information.
    """
    
    def __init__(self, xml_dir: str):
        """Initialize the XML processor.
        
        Args:
            xml_dir: Directory containing XML files to process
        """
        self.xml_dir = xml_dir
        self.results = []
        
    def find_xml_files(self, limit: Optional[int] = None) -> List[str]:
        """Find all XML files in the specified directory.
        
        Args:
            limit: Optional maximum number of files to return (for testing)
            
        Returns:
            List of paths to XML files
        """
        xml_files = []
        
        logger.info(f"Searching for XML files in {self.xml_dir}")
        for root, _, files in os.walk(self.xml_dir):
            for file in files:
                if file.lower().endswith('.xml'):
                    xml_files.append(os.path.join(root, file))
                    
        logger.info(f"Found {len(xml_files)} XML files")
        
        if limit and len(xml_files) > limit:
            logger.info(f"Limiting to {limit} XML files for processing")
            return xml_files[:limit]
            
        return xml_files
    
    def process_xml_files(self, max_workers: Optional[int] = None, 
                         limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Process XML files in parallel using ThreadPoolExecutor.
        
        Args:
            max_workers: Maximum number of threads to use
            limit: Maximum number of files to process
            
        Returns:
            List of dictionaries containing extracted metadata
        """
        xml_files = self.find_xml_files(limit)
        
        if not xml_files:
            logger.warning("No XML files found to process")
            return []
            
        logger.info(f"Processing {len(xml_files)} XML files with {max_workers or 'default'} workers")
        
        # Process files in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Use tqdm for a progress bar
            results = list(tqdm(
                executor.map(self._process_single_xml, xml_files),
                total=len(xml_files),
                desc="Processing XML files"
            ))
            
        # Filter out None results (failed processing)
        valid_results = [r for r in results if r is not None]
        logger.info(f"Successfully processed {len(valid_results)} out of {len(xml_files)} XML files")
        
        self.results = valid_results
        return valid_results
    
    def _process_single_xml(self, xml_path: str) -> Optional[Dict[str, Any]]:
        """Process a single XML file to extract metadata.
        
        Args:
            xml_path: Path to the XML file
            
        Returns:
            Dictionary with extracted metadata or None if processing failed
        """
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            # Extract metadata
            audio_filename = self._extract_audio_filename(root)
            isrc = self._extract_isrc(root)
            track_title = self._extract_track_title(root)
            artist = self._extract_artist(root)
            
            if not audio_filename:
                logger.warning(f"No audio filename found in {xml_path}")
                return None
                
            return {
                'xml_path': xml_path,
                'audio_filename': audio_filename,
                'isrc': isrc,
                'track_title': track_title,
                'artist': artist
            }
        except Exception as e:
            logger.error(f"Error processing {xml_path}: {str(e)}")
            return None
    
    def _extract_audio_filename(self, root: ET.Element) -> Optional[str]:
        """Extract audio filename from XML.
        
        Implementation depends on the specific structure of your XML files.
        This is a placeholder that should be customized.
        
        Args:
            root: Root element of the XML tree
            
        Returns:
            Audio filename if found, None otherwise
        """
        # Try common tag and attribute patterns - adjust based on your XML structure
        for pattern in [
            # Direct tag matches
            './/FileName', './/AudioFileName', './/SoundRecording/FileName',
            # Attributes
            './/*[@filename]', './/*[@audioFileName]',
            # Try with namespaces (if your XML uses them)
            './/{*}FileName', './/{*}AudioFileName', './/{*}SoundRecording/{*}FileName'
        ]:
            elements = root.findall(pattern)
            if elements:
                # If it's a tag, get text; if attribute, get the attribute value
                if pattern.startswith('.//*[@'):
                    attr_name = pattern[5:-1]  # Extract attribute name from [@ ... ]
                    return elements[0].get(attr_name)
                else:
                    return elements[0].text
                    
        # If we got here, try to find any filename-like element or attribute
        for elem in root.iter():
            # Check element tag
            if any(name in elem.tag.lower() for name in ['filename', 'audiofilename', 'soundrecording']):
                if elem.text and '.wav' in elem.text.lower():
                    return elem.text
                    
            # Check attributes
            for attr_name, attr_value in elem.attrib.items():
                if (any(name in attr_name.lower() for name in ['filename', 'file', 'audio']) and 
                    attr_value and '.wav' in attr_value.lower()):
                    return attr_value
                    
        return None
    
    def _extract_isrc(self, root: ET.Element) -> Optional[str]:
        """Extract ISRC from XML.
        
        Args:
            root: Root element of the XML tree
            
        Returns:
            ISRC if found, None otherwise
        """
        # Try common patterns - adjust based on your XML structure
        for pattern in ['.//ISRC', './/*[@isrc]', './/{*}ISRC']:
            elements = root.findall(pattern)
            if elements:
                if pattern.startswith('.//*[@'):
                    return elements[0].get('isrc')
                else:
                    return elements[0].text
        
        # Try to find any ISRC-like element
        for elem in root.iter():
            if 'isrc' in elem.tag.lower():
                return elem.text
                
            for attr_name, attr_value in elem.attrib.items():
                if 'isrc' in attr_name.lower():
                    return attr_value
                    
        return None
    
    def _extract_track_title(self, root: ET.Element) -> Optional[str]:
        """Extract track title from XML.
        
        Args:
            root: Root element of the XML tree
            
        Returns:
            Track title if found, None otherwise
        """
        # Try common patterns - adjust based on your XML structure
        for pattern in [
            './/Title', './/TrackTitle', './/SoundRecording/Title',
            './/*[@title]', './/*[@trackTitle]',
            './/{*}Title', './/{*}TrackTitle'
        ]:
            elements = root.findall(pattern)
            if elements:
                if pattern.startswith('.//*[@'):
                    attr_name = pattern[5:-1] 
                    return elements[0].get(attr_name)
                else:
                    return elements[0].text
        
        # Try to find any title-like element
        for elem in root.iter():
            if any(name in elem.tag.lower() for name in ['title', 'tracktitle']):
                return elem.text
                
            for attr_name, attr_value in elem.attrib.items():
                if any(name in attr_name.lower() for name in ['title', 'tracktitle']):
                    return attr_value
                    
        return None
    
    def _extract_artist(self, root: ET.Element) -> Optional[str]:
        """Extract artist name from XML.
        
        Args:
            root: Root element of the XML tree
            
        Returns:
            Artist name if found, None otherwise
        """
        # Try common patterns - adjust based on your XML structure
        for pattern in [
            './/Artist', './/ArtistName', './/Performer/Name',
            './/*[@artist]', './/*[@artistName]',
            './/{*}Artist', './/{*}ArtistName', './/{*}Performer/{*}Name'
        ]:
            elements = root.findall(pattern)
            if elements:
                if pattern.startswith('.//*[@'):
                    attr_name = pattern[5:-1]
                    return elements[0].get(attr_name)
                else:
                    return elements[0].text
        
        # Try to find any artist-like element
        for elem in root.iter():
            if any(name in elem.tag.lower() for name in ['artist', 'performer', 'creator']):
                return elem.text
                
            for attr_name, attr_value in elem.attrib.items():
                if any(name in attr_name.lower() for name in ['artist', 'performer', 'creator']):
                    return attr_value
                    
        return None