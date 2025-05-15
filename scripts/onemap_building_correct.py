#!/usr/bin/env python3
"""
OneMap SG Building Correction Script

This script processes the differences file to:
1. Remove duplicate postal codes
2. Apply consistent naming conventions
"""

import pandas as pd
import re
import logging
import argparse
import time
from collections import Counter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BuildingCorrector:
    """Class to deduplicate and correct naming conventions for building data"""
    
    def __init__(self, input_file, output_file):
        """Initialize with input and output file paths"""
        self.input_file = input_file
        self.output_file = output_file
        self.df = None
        self.result_df = None
        self.duplicate_postal_codes = []
        
        # Define regex patterns for analysis
        self.non_parent_blk_patterns = [
            r'[0-9]+-[0-9]+',         # Range format like "1-5"
            r'[0-9]+[A-Za-z]+',       # Number followed by letter like "123A"
            r'[A-Za-z]+[0-9]+',       # Letter followed by number like "B123"
        ]
        
        self.non_parent_name_patterns = [
            r'#[0-9]+-[0-9]+',                               # Unit format like "#01-23"
            r'(?:Unit|Apt|Room)\s*[0-9]+',                   # Unit keywords
            r'(?:Block|Blk|Tower|Phase|Level|Floor)\s*[A-Za-z0-9]+', # Block/tower identifiers
            r'\b[Ll]evel\s*[0-9]+\b',                        # Level indicator
            r'\b[Ll][0-9]+\b',                               # Level shorthand (L1, etc)
            r'\b[Ff]loor\s*[0-9]+\b',                        # Floor indicator
        ]
        
        self.parent_building_keywords = [
            r'\b(?:Mall|Plaza|Centre|Center|Complex|Building|Tower|House|Court|Place)\b',
            r'\b(?:Terminal|Station|Hub|Interchange|Airport|Terminus)\b',
            r'\b(?:School|University|College|Institute|Academy)\b',
            r'\b(?:Hospital|Medical|Clinic|Healthcare)\b',
            r'\b(?:Hotel|Resort|Apartment|Condo|Condominium)\b'
        ]
        
        # Define non-residential patterns
        self.non_residential_patterns = [
            # Schools & Education
            r'(school|college|university|institute|polytechnic|campus|kindergarten|preschool|childcare|tuition|academy|training|education|learning|nursery|montessori|international school)',
            
            # Transport
            r'(mrt|lrt|station|interchange|terminal|depot|expressway|transit|bus stop|taxi stand|port|airport|jetty|ferry|carpark|car park|parking|bus station|bus terminal|stop)',
            
            # Community facilities
            r'(community center|cc$|cc\s|community club|rc$|residents committee|residents\' committee|neighborhood center)',
            
            # Sports facilities
            r'(stadium|sports|swimming|pool|recreation|club|golf|court|field|fitness|gym|sports hall|sports complex)',
            
            # Healthcare
            r'(hospital|clinic|medical|healthcare|health centre|polyclinic|dental|pharmacy|nursing home|rehabilitation|dialysis|laboratory|surgery)',
            
            # Shopping & Commercial
            r'(mall|plaza|centre|center|tower|building|complex|retail|supermarket|hypermarket|bank|financial|insurance|cinema|theatre|entertainment|outlet|showroom|store|cafe|bakery|salon|atm|spa|gym|fitness|food court|hawker|market)',
            
            # Religious
            r'(temple|church|mosque|synagogue|chapel|cathedral|religious|shrine|worship)',
            
            # Government & Public Services
            r'(ministry|government|police|fire|town council|parliament|court|judiciary|library|post office|checkpoint)',
            
            # Parks & Recreation
            r'(park|garden|playground|nature reserve|reservoir|national park|botanical|bird park|zoo)',
            
            # Commercial Landmarks
            r'(opp\s+[a-z\s]+(?:mall|plaza|centre|center|junction|point|complex)|\bnear\s+[a-z\s]+(?:mall|plaza|centre|center|junction|point|complex)|\bbefore\s+[a-z\s]+(?:mall|plaza|centre|center|junction|point|complex)|\bafter\s+[a-z\s]+(?:mall|plaza|centre|center|junction|point|complex))',
            
            # Specific establishment patterns
            r'(hotel|restaurant|café|cafe|food|dining|eatery|coffeeshop|kopitiam|hawker|canteen)',
            
            # Industrial
            r'(factory|warehouse|industrial|business park|tech park|techpark|logistics|manufacturing|workshop|shipyard|refinery)',
            
            # Multi-storey car park and variants
            r'(multi[\s\-]?storey car park|mscp|multi[\s\-]?level car park|car park|mechanized car park)',
        ]
        
        # Compile patterns
        self.non_residential_regexes = [re.compile(pattern, re.IGNORECASE) for pattern in self.non_residential_patterns]
        
        # Singapore abbreviations
        self.singapore_abbreviations = {
            # Transport
            'MRT': 'Mass Rapid Transit',
            'LRT': 'Light Rail Transit',
            'CTE': 'Central Expressway',
            'PIE': 'Pan Island Expressway',
            'SLE': 'Seletar Expressway',
            'BKE': 'Bukit Timah Expressway',
            'KPE': 'Kallang-Paya Lebar Expressway',
            'TPE': 'Tampines Expressway',
            'AYE': 'Ayer Rajah Expressway',
            'MCE': 'Marina Coastal Expressway',
            'ECP': 'East Coast Parkway',
            'MSCP': 'Multi Storey Car Park',
            
            # Education
            'NUS': 'National University of Singapore',
            'NTU': 'Nanyang Technological University',
            'SMU': 'Singapore Management University',
            'SUTD': 'Singapore University of Technology and Design',
            'SIM': 'Singapore Institute of Management',
            'NYP': 'Nanyang Polytechnic',
            'SP': 'Singapore Polytechnic',
            'TP': 'Temasek Polytechnic',
            'RP': 'Republic Polytechnic',
            'NP': 'Ngee Ann Polytechnic',
            'ITE': 'Institute of Technical Education',
            'NAFA': 'Nanyang Academy of Fine Arts',
            'SUSS': 'Singapore University of Social Sciences',
        }
        
        # Define strong non-residential indicators
        self.strong_non_residential_indicators = [
            'school', 'college', 'university', 'hospital', 'mall', 'plaza', 'centre', 'center', 
            'hotel', 'station', 'interchange', 'terminal', 'park', 'carpark', 'multi-storey',
            'church', 'temple', 'mosque', 'court', 'sports', 'stadium', 'hall', 'community',
            'library', 'theatre', 'cinema', 'office', 'bank', 'restaurant', 'shop', 'store'
        ]
    
    def load_data(self):
        """Load the dataset and prepare for processing"""
        logger.info(f"Loading data from {self.input_file}")
        try:
            self.df = pd.read_csv(self.input_file)
            logger.info(f"Loaded {len(self.df)} records")
            
            # Ensure postal_code is string type
            self.df['postal_code'] = self.df['postal_code'].astype(str)
            
            # Find duplicate postal codes
            duplicates = self.df['postal_code'].duplicated(keep=False)
            self.duplicate_postal_codes = self.df[duplicates]['postal_code'].unique()
            
            logger.info(f"Found {len(self.duplicate_postal_codes)} unique postal codes with duplicates")
            logger.info(f"Total records with duplicate postal codes: {sum(duplicates)}")
            
            return True
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return False
    
    def is_parent_building_blk(self, blk_no):
        """
        Check if the block number indicates a parent building.
        
        Args:
            blk_no: The block number to check
            
        Returns:
            Boolean and score: (is_parent, score)
        """
        if pd.isna(blk_no) or blk_no == '':
            return (False, 0)
        
        blk_str = str(blk_no).strip()
        
        # Check if the block matches any non-parent pattern
        for pattern in self.non_parent_blk_patterns:
            if re.search(pattern, blk_str):
                return (False, 0)
        
        # Check if it's a simple numeric block (parent indicator)
        if re.match(r'^[0-9]+$', blk_str):
            # Simple numeric blocks are likely parent buildings
            # Lower numbers get higher scores
            try:
                num = int(blk_str)
                if num < 10:
                    return (True, 3)  # Very likely parent (single digit)
                elif num < 100:
                    return (True, 2)  # Likely parent (double digit)
                else:
                    return (True, 1)  # Possible parent (larger number)
            except:
                return (True, 1)  # Just digits but couldn't parse as int
        
        return (False, 0)  # Default - not a parent building block pattern
    
    def is_parent_building_name(self, name):
        """
        Check if the building name indicates a parent building.
        
        Args:
            name: The building name to check
            
        Returns:
            Boolean and score: (is_parent, score)
        """
        if pd.isna(name) or name == '':
            return (False, 0)
        
        name_str = str(name).strip()
        
        # Check if the name matches any non-parent pattern
        for pattern in self.non_parent_name_patterns:
            if re.search(pattern, name_str):
                return (False, 0)
        
        # Check for parent building keywords
        parent_score = 0
        for pattern in self.parent_building_keywords:
            if re.search(pattern, name_str, re.IGNORECASE):
                parent_score += 1
        
        if parent_score > 0:
            return (True, parent_score)
        
        return (False, 0)  # Default - not identified as parent name
    
    def calculate_parent_building_score(self, row):
        """
        Calculate a score indicating how likely this record represents a parent building.
        Higher scores indicate higher likelihood of being a parent building.
        """
        score = 0
        
        # Check block number
        is_parent_blk, blk_score = self.is_parent_building_blk(row['blk_no'])
        if is_parent_blk:
            score += blk_score * 5  # Block number is a strong indicator
        
        # Check building name
        is_parent_name, name_score = self.is_parent_building_name(row['name'])
        if is_parent_name:
            score += name_score * 3  # Name is also a good indicator
        
        # Completeness of data
        for col in ['street', 'name', 'lat', 'lon']:
            if not pd.isna(row[col]) and row[col] != '':
                score += 1  # Complete data fields
        
        # Shorter street names often indicate main buildings
        if not pd.isna(row['street']) and row['street'] != '':
            street_length = len(str(row['street']))
            if street_length < 20:
                score += 2  # Short street name
            elif street_length < 30:
                score += 1  # Medium street name
        
        return score
    
    def deduplicate(self):
        """
        Deduplicate the dataset by selecting parent buildings for duplicate postal codes.
        """
        logger.info("Starting deduplication process...")
        
        # Create a copy of the dataframe for processing
        df_with_scores = self.df.copy()
        
        # Calculate parent building scores for all records
        logger.info("Calculating parent building scores...")
        df_with_scores['parent_score'] = df_with_scores.apply(
            self.calculate_parent_building_score, axis=1
        )
        
        # Prepare result dataframe
        result_df = pd.DataFrame()
        
        # Process non-duplicate records
        non_duplicates = ~self.df['postal_code'].isin(self.duplicate_postal_codes)
        result_df = pd.concat([result_df, self.df[non_duplicates]], ignore_index=True)
        logger.info(f"Added {sum(non_duplicates)} non-duplicate records to result")
        
        # Group duplicate records by postal code
        duplicate_groups = []
        for postal_code in self.duplicate_postal_codes:
            group = df_with_scores[df_with_scores['postal_code'] == postal_code].copy()
            duplicate_groups.append(group)
        
        # Process each duplicate group
        selected_from_duplicates = 0
        for group in duplicate_groups:
            # Sort by parent score (descending)
            group = group.sort_values('parent_score', ascending=False)
            
            # Select the record with highest parent score
            selected_record = group.iloc[0].drop('parent_score')
            result_df = pd.concat([result_df, pd.DataFrame([selected_record])], ignore_index=True)
            selected_from_duplicates += 1
        
        logger.info(f"Selected {selected_from_duplicates} parent buildings from duplicate groups")
        logger.info(f"Total records in deduplicated dataset: {len(result_df)}")
        
        self.result_df = result_df
        return result_df
    
    def contains_non_residential_abbreviation(self, text):
        """Check if text contains non-residential Singapore abbreviations."""
        if not isinstance(text, str):
            return False
        
        text_upper = text.upper()
        
        for abbr in self.singapore_abbreviations.keys():
            pattern = r'\b' + re.escape(abbr) + r'\b'
            if re.search(pattern, text_upper):
                return True
        
        return False

    def is_non_residential(self, name, street):
        """Determine if a building is non-residential based on its name and street."""
        # Convert to string and handle NaN values
        name = str(name) if pd.notna(name) else ''
        street = str(street) if pd.notna(street) else ''
        
        # Combined text to check
        text = (name + ' ' + street).lower()
        
        # Check for strong non-residential indicators
        for indicator in self.strong_non_residential_indicators:
            pattern = r'\b' + re.escape(indicator) + r'\b'
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        # Check for non-residential patterns
        for regex in self.non_residential_regexes:
            if regex.search(text):
                return True
        
        # Check for Singapore abbreviations
        if self.contains_non_residential_abbreviation(name + ' ' + street):
            return True
        
        # Special cases for transit stops
        if "opp " in text.lower() or "bef " in text.lower() or "aft " in text.lower() or "bus stop" in text.lower():
            # Look for words that might follow "opp", "bef", "aft" that indicate non-residential
            opp_pattern = r'(opp|bef|aft)\s+([a-z0-9\s]+)'
            match = re.search(opp_pattern, text.lower())
            if match:
                location = match.group(2)
                # Check if the location contains any non-residential indicators
                for indicator in self.strong_non_residential_indicators:
                    if indicator.lower() in location:
                        return True
        
        # Default: assume residential
        return False
    
    def proper_case(self, text):
        """Convert text to proper case, preserving certain abbreviations."""
        if not isinstance(text, str) or text.strip() == '':
            return ''
        
        # List of words that should remain uppercase
        uppercase_words = set([abbr for abbr in self.singapore_abbreviations.keys()])
        # List of words that should remain lowercase
        lowercase_words = set(['a', 'an', 'the', 'and', 'but', 'or', 'for', 'nor', 
                              'on', 'at', 'to', 'from', 'by', 'of', 'in'])
        
        # Split the text into words
        words = text.split()
        result = []
        
        for i, word in enumerate(words):
            # Check for abbreviations that should remain uppercase
            if word.upper() in uppercase_words:
                result.append(word.upper())
            # First word or not in lowercase_words list should be capitalized
            elif i == 0 or word.lower() not in lowercase_words:
                result.append(word.capitalize())
            # Words in lowercase_words list should remain lowercase
            else:
                result.append(word.lower())
        
        return ' '.join(result)
    
    def format_name(self, row):
        """Format the building name based on whether it's residential or non-residential."""
        name = str(row['name']).strip() if pd.notna(row['name']) else ''
        blk_no = str(row['blk_no']).strip() if pd.notna(row['blk_no']) else ''
        street = str(row['street']).strip() if pd.notna(row['street']) else ''
        is_non_res = row['is_non_residential']
        
        # For non-residential buildings, use the name directly if available
        if is_non_res:
            if name and name.lower() not in ['nil', 'nan', '']:
                words = name.split()
                # Title case each word except the first one if it's a block number
                if words and re.match(r'^\d+[A-Za-z]?$', words[0]):
                    # Keep the block number as is, title case the rest
                    return words[0] + " " + self.proper_case(' '.join(words[1:]))
                else:
                    return self.proper_case(name)
            
            # If name is not available, try to extract a meaningful name from the street
            if street and street.lower() not in ['nil', 'nan', '']:
                # For transit stops with patterns like "Opp X" or "Bef X"
                opp_pattern = r'(opp|bef|aft)\s+([a-z0-9\s]+)'
                match = re.search(opp_pattern, street.lower())
                if match:
                    location = match.group(0)
                    return self.proper_case(location)
                else:
                    # Just use the street as name, but remove the block number if it's at the start
                    words = street.split()
                    if words and blk_no and words[0] == blk_no:
                        return self.proper_case(' '.join(words[1:]))
                    return self.proper_case(street)
            
            # If both name and street are unusable, return a generic name
            return "Unnamed Non-residential Location"
        
        # For residential buildings, follow HDB format: "Block X Street Name"
        else:
            if blk_no and blk_no.lower() not in ['nil', 'nan', '']:
                if street and street.lower() not in ['nil', 'nan', '']:
                    # Keep blk_no as is, then proper case the street
                    return blk_no + " " + self.proper_case(street)
                else:
                    # Just the block number
                    return blk_no
            elif name and name.lower() not in ['nil', 'nan', '']:
                # Use building name with proper case
                words = name.split()
                if words and re.match(r'^\d+[A-Za-z]?$', words[0]):
                    # Keep the block number as is, title case the rest
                    return words[0] + " " + self.proper_case(' '.join(words[1:]))
                else:
                    return self.proper_case(name)
            elif street and street.lower() not in ['nil', 'nan', '']:
                # Just the street with proper case
                return self.proper_case(street)
            else:
                return "Unnamed Location"
    
    def format_address(self, row):
        """Format the address according to standards."""
        blk_no = str(row['blk_no']).strip() if pd.notna(row['blk_no']) else ''
        street = str(row['street']).strip() if pd.notna(row['street']) else ''
        name = str(row['name']).strip() if pd.notna(row['name']) else ''
        postal_code = str(row['postal_code']).strip() if pd.notna(row['postal_code']) else ''
        is_non_res = row['is_non_residential']
        
        # Construct address components
        address_parts = []
        
        # Add block number if available (keep as-is)
        if blk_no and blk_no.lower() not in ['nil', 'nan', '']:
            address_parts.append(blk_no)
        
        # Add street name if available (proper case)
        if street and street.lower() not in ['nil', 'nan', '']:
            address_parts.append(self.proper_case(street))
        
        # Add building name for non-residential buildings (proper case)
        if is_non_res and name and name.lower() not in ['nil', 'nan', '']:
            # Avoid duplication if name is already in street
            if name.lower() not in street.lower():
                address_parts.append(self.proper_case(name))
        
        # Construct the base address
        base_address = ' '.join(address_parts)
        
        # Add postal code with 'Singapore' prefix
        if postal_code and postal_code.lower() not in ['nil', 'nan', '']:
            # Format: "Base Address, Singapore POSTAL_CODE"
            full_address = f"{base_address}, Singapore {postal_code}"
        else:
            full_address = base_address
        
        return full_address
    
    def apply_naming_conventions(self):
        """Apply naming conventions to the deduplicated dataset."""
        if self.result_df is None:
            raise ValueError("No deduplicated data available. Run deduplicate() first.")
        
        logger.info("Applying naming conventions...")
        
        # Classify each building as residential or non-residential
        self.result_df['is_non_residential'] = self.result_df.apply(
            lambda row: self.is_non_residential(row['name'], row['street']), axis=1
        )
        
        # Count residential vs non-residential
        non_residential_count = self.result_df['is_non_residential'].sum()
        residential_count = len(self.result_df) - non_residential_count
        logger.info(f"Classified buildings: {residential_count} residential, {non_residential_count} non-residential")
        
        # Format names and addresses
        self.result_df['name_formatted'] = self.result_df.apply(self.format_name, axis=1)
        self.result_df['address_formatted'] = self.result_df.apply(self.format_address, axis=1)
        
        logger.info("Naming conventions applied successfully")
        return self.result_df
    
    def save_result(self):
        """Save the corrected dataset to CSV file"""
        if self.result_df is None:
            raise ValueError("No result data available. Run the process first.")
            
        # Save to CSV
        self.result_df.to_csv(self.output_file, index=False)
        logger.info(f"Corrected dataset saved to {self.output_file}")
        
        # Generate a summary of the corrections
        total_original = len(self.df) if self.df is not None else 0
        total_corrected = len(self.result_df)
        
        logger.info("=" * 50)
        logger.info("CORRECTION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Original records: {total_original}")
        logger.info(f"Corrected records: {total_corrected}")
        
        if total_original > 0:
            logger.info(f"Records removed: {total_original - total_corrected} ({(total_original - total_corrected)/total_original*100:.2f}%)")
        
        if 'is_non_residential' in self.result_df.columns:
            non_residential_count = self.result_df['is_non_residential'].sum()
            residential_count = len(self.result_df) - non_residential_count
            logger.info(f"Residential buildings: {residential_count} ({residential_count/len(self.result_df)*100:.2f}%)")
            logger.info(f"Non-residential buildings: {non_residential_count} ({non_residential_count/len(self.result_df)*100:.2f}%)")
        
        logger.info("=" * 50)
        
        return True
    
    def run(self):
        """Run the entire correction process"""
        start_time = time.time()
        
        if not self.load_data():
            logger.error("Failed to load data. Aborting.")
            return False
        
        self.deduplicate()
        self.apply_naming_conventions()
        self.save_result()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Complete correction process finished in {elapsed_time:.2f} seconds")
        
        return True

def main():
    """Main function to handle command line arguments and execute correction process"""
    parser = argparse.ArgumentParser(description='Deduplicate and correct naming conventions for building data')
    parser.add_argument('--input_file', type=str, required=True,
                        help='Path to input differences CSV file')
    parser.add_argument('--output_file', type=str, required=True,
                        help='Path for output corrected CSV file')
    
    args = parser.parse_args()
    
    # Create corrector instance
    corrector = BuildingCorrector(args.input_file, args.output_file)
    
    # Run the correction process
    corrector.run()

if __name__ == "__main__":
    main()
