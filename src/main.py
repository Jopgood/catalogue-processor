#!/usr/bin/env python3
import argparse
import logging
import os
import time
from pathlib import Path
from typing import Optional, Dict, Any, List

from src.xml_processor import XMLProcessor
from src.manifest_handler import ManifestHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("catalogue_process.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def process_catalogue(manifest_path: str, xml_dir: str, output_path: str, 
                    max_workers: Optional[int] = None, limit: Optional[int] = None) -> str:
    """Process XML files and match them with manifest entries.
    
    Args:
        manifest_path: Path to the manifest file
        xml_dir: Directory containing XML files
        output_path: Path for the output file
        max_workers: Maximum number of worker threads to use
        limit: Maximum number of files to process (for testing)
        
    Returns:
        Path to the saved output file
    """
    start_time = time.time()
    logger.info(f"Starting catalogue processing")
    
    # Step 1: Load the manifest
    manifest_handler = ManifestHandler(manifest_path)
    manifest_handler.load_manifest()
    
    # Step 2: Process XML files
    xml_processor = XMLProcessor(xml_dir)
    xml_results = xml_processor.process_xml_files(max_workers=max_workers, limit=limit)
    
    if not xml_results:
        logger.warning("No valid XML processing results found")
        return None
    
    # Step 3: Match XML metadata with manifest entries
    logger.info("Matching XML metadata with manifest entries")
    matched_metadata = []
    unmatched_count = 0
    
    for result in xml_results:
        audio_filename = result.get('audio_filename')
        if not audio_filename:
            continue
            
        manifest_entry = manifest_handler.find_audio_file(audio_filename)
        
        if manifest_entry:
            # Add metadata to the matched entry
            metadata = {
                'audio_filename': audio_filename,
                'isrc': result.get('isrc'),
                'track_title': result.get('track_title'),
                'artist': result.get('artist'),
                'xml_path': result.get('xml_path')
            }
            matched_metadata.append(metadata)
        else:
            unmatched_count += 1
    
    logger.info(f"Matched {len(matched_metadata)} files, failed to match {unmatched_count} files")
    
    # Step 4: Update the manifest with metadata
    updated_manifest = manifest_handler.update_manifest_with_metadata(matched_metadata)
    
    # Step 5: Save the updated manifest
    output_file = manifest_handler.save_updated_manifest(output_path)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Processing completed in {elapsed_time:.2f} seconds")
    logger.info(f"Results saved to {output_file}")
    
    return output_file


def main():
    """Main entry point for the command-line interface."""
    parser = argparse.ArgumentParser(
        description='Process XML catalogue files and match with manifest'
    )
    
    parser.add_argument('--manifest', required=True,
                       help='Path to the manifest file (CSV, JSON, or Excel)')
    parser.add_argument('--xml-dir', required=True,
                       help='Directory containing XML files to process')
    parser.add_argument('--output', required=True,
                       help='Path for the output file (format determined by extension)')
    parser.add_argument('--max-workers', type=int, default=os.cpu_count(),
                       help='Maximum number of worker threads (default: CPU count)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit the number of XML files to process (for testing)')
    
    args = parser.parse_args()
    
    # Validate input paths
    if not os.path.isfile(args.manifest):
        logger.error(f"Manifest file not found: {args.manifest}")
        return 1
        
    if not os.path.isdir(args.xml_dir):
        logger.error(f"XML directory not found: {args.xml_dir}")
        return 1
        
    # Create output directory if it doesn't exist
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        output_file = process_catalogue(
            args.manifest, 
            args.xml_dir,
            args.output,
            max_workers=args.max_workers,
            limit=args.limit
        )
        
        if output_file:
            logger.info(f"Successfully processed catalogue. Results saved to {output_file}")
            return 0
        else:
            logger.error("Processing failed")
            return 1
            
    except Exception as e:
        logger.exception(f"Error processing catalogue: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())