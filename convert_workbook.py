import zipfile
import logging
from pathlib import Path
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def convert_twbx_to_twb(twbx_path: str | Path, output_path: str | Path | None = None) -> Path:
    """
    Convert a Tableau packaged workbook (.twbx) to an unpackaged workbook (.twb).
    
    Args:
        twbx_path (str | Path): Path to the input .twbx file
        output_path (str | Path | None): Path where the .twb file should be saved.
            If None, will use the same name as input but with .twb extension
    
    Returns:
        Path: Path to the created .twb file
    
    Raises:
        ValueError: If the input file is not a .twbx file or if no .twb file is found in the package
        FileNotFoundError: If the input file doesn't exist
    """
    # Convert paths to Path objects
    twbx_path = Path(twbx_path)
    
    # Validate input file
    if not twbx_path.exists():
        raise FileNotFoundError(f"Input file not found: {twbx_path}")
    
    if twbx_path.suffix.lower() != '.twbx':
        raise ValueError(f"Input file must be a .twbx file, got: {twbx_path.suffix}")
    
    # Set default output path if not provided
    if output_path is None:
        output_path = twbx_path.with_suffix('.twb')
    else:
        output_path = Path(output_path)
        if output_path.suffix.lower() != '.twb':
            output_path = output_path.with_suffix('.twb')
    
    logger.info(f"Converting {twbx_path} to {output_path}")
    
    try:
        with zipfile.ZipFile(twbx_path, 'r') as zip_ref:
            # Find the .twb file in the package
            twb_files = [f for f in zip_ref.namelist() if f.endswith('.twb')]
            if not twb_files:
                raise ValueError("No .twb file found in TWBX package")
            
            twb_file = twb_files[0]
            logger.info(f"Found TWB file in package: {twb_file}")
            
            # Extract the .twb file
            with zip_ref.open(twb_file) as source, open(output_path, 'wb') as target:
                shutil.copyfileobj(source, target)
            
            logger.info(f"Successfully converted {twbx_path} to {output_path}")
            return output_path
            
    except zipfile.BadZipFile:
        raise ValueError(f"Invalid TWBX file: {twbx_path}")
    except Exception as e:
        raise RuntimeError(f"Error converting TWBX to TWB: {str(e)}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python convert_workbook.py <twbx_file> [output_path]")
        sys.exit(1)
    
    try:
        twbx_path = sys.argv[1]
        output_path = sys.argv[2] if len(sys.argv) > 2 else None
        result = convert_twbx_to_twb(twbx_path, output_path)
        print(f"Successfully converted to: {result}")
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1) 