import logging
from hdfs import InsecureClient
import pandas as pd
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# HDFS Configuration
hdfs_client = InsecureClient('http://localhost:9870', user='atul')
input_base = '/user/atul/news_data/input/'
output_base = '/user/atul/news_data/output/'

def clean_dataframe(df):
    """Perform all data cleaning operations on the dataframe"""
    try:
        # Remove columns starting with 'Unnamed' or named 'ID'
        columns_to_remove = [
            col for col in df.columns 
            if col.startswith('Unnamed') or col == 'ID'
        ]
        df = df.drop(columns=columns_to_remove, errors='ignore')
        
        # Remove rows with any missing values
        df = df.dropna()
        
        # Remove duplicate rows
        df = df.drop_duplicates()
        
        return df
    
    except Exception as e:
        logger.error(f"Data cleaning error: {e}")
        raise

def process_files():
    """Main processing function for HDFS files"""
    try:
        # Create output directory if it doesn't exist
        if not hdfs_client.status(output_base, strict=False):
            hdfs_client.makedirs(output_base)
            logger.info(f"Created output directory: {output_base}")
    except Exception as e:
        logger.error(f"Output directory creation failed: {e}")
        return

    # Process files sequentially
    for file_index in range(1, 872):
        input_path = os.path.join(input_base, f'news_data_{file_index}.json')
        output_path = os.path.join(output_base, f'news_data_{file_index}.json')

        try:
            # skip if input file doesn't exist
            if not hdfs_client.status(input_path, strict=False):
                logger.warning(f"Skipping missing file: {input_path}")
                continue

            # read data from HDFS
            with hdfs_client.read(input_path) as reader:
                df = pd.read_json(reader, lines=True)
            
            logger.info(f"Processing {input_path} | Initial rows: {len(df)}")

            # clean the dataframe
            cleaned_df = clean_dataframe(df)
            
            # write processed data to HDFS
            with hdfs_client.write(output_path, overwrite=True) as writer:
                cleaned_df.to_json(
                    writer,
                    orient='records',
                    lines=True,
                    date_format='iso',
                    date_unit='s'
                )
            
            logger.info(f"Saved cleaned data to {output_path} | Final rows: {len(cleaned_df)}")

        except Exception as e:
            logger.error(f"Failed processing {input_path}: {str(e)}")
            continue

if __name__ == '__main__':
    logger.info("Starting HDFS processing pipeline")
    process_files()
    logger.info("Processing complete")