#!/usr/bin/env python3
"""
OneMap SG Building Comparison Script

This script compares two OneMap datasets to identify new buildings and changes
in existing buildings.
"""

import pandas as pd
import logging
import argparse
from datetime import datetime
import math

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OnemapComparator:
    """Class to compare two OneMap datasets and identify differences"""
    
    def __init__(self, previous_file, current_file, diff_output=None, location_threshold=300):
        """
        Initialize with paths to previous and current data files.
        
        Args:
            previous_file: Path to previous OneMap dataset CSV
            current_file: Path to current OneMap dataset CSV
            diff_output: Path for output differences CSV file
            location_threshold: Distance threshold in meters to detect location changes (default: 300)
        """
        self.previous_file = previous_file
        self.current_file = current_file
        self.location_threshold = location_threshold  # in meters
        
        if diff_output:
            self.diff_output = diff_output
        else:
            prev_date = previous_file.split('_')[-1].split('.')[0]
            curr_date = current_file.split('_')[-1].split('.')[0]
            self.diff_output = f"data/differences_onemap_{prev_date}-{curr_date}.csv"
            
        self.previous_data = None
        self.current_data = None
        self.differences = None
        
        # Statistics
        self.stats = {
            'new_buildings': 0,
            'name_changes': 0,
            'location_changes': 0,
            'total_changes': 0
        }
    
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """
        Calculate the Haversine distance between two points in meters.
        
        Args:
            lat1, lon1: Coordinates of first point
            lat2, lon2: Coordinates of second point
            
        Returns:
            Distance in meters
        """
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371000  # Radius of Earth in meters
        distance = c * r
        
        return distance
        
    def load_data(self):
        """Load previous and current datasets"""
        logger.info(f"Loading previous data from {self.previous_file}")
        self.previous_data = pd.read_csv(self.previous_file)
        logger.info(f"Previous dataset: {len(self.previous_data)} records")
        
        logger.info(f"Loading current data from {self.current_file}")
        self.current_data = pd.read_csv(self.current_file)
        logger.info(f"Current dataset: {len(self.current_data)} records")
        
        # Ensure postal_code is string type
        self.previous_data['postal_code'] = self.previous_data['postal_code'].astype(str)
        self.current_data['postal_code'] = self.current_data['postal_code'].astype(str)
        
        return True
        
    def compare_datasets(self):
        """Compare datasets to identify differences"""
        if self.previous_data is None or self.current_data is None:
            raise ValueError("Datasets must be loaded before comparison")
            
        logger.info("Comparing datasets to identify differences...")
        
        # Create composite keys for matching (postal_code + blk_no)
        self.previous_data['composite_key'] = self.previous_data['postal_code'] + '_' + self.previous_data['blk_no'].astype(str)
        self.current_data['composite_key'] = self.current_data['postal_code'] + '_' + self.current_data['blk_no'].astype(str)
        
        # Get sets of composite keys
        previous_keys = set(self.previous_data['composite_key'])
        current_keys = set(self.current_data['composite_key'])
        
        # Find new buildings (in current but not in previous)
        new_building_keys = current_keys - previous_keys
        self.stats['new_buildings'] = len(new_building_keys)
        logger.info(f"Found {self.stats['new_buildings']} new buildings")
        
        # Extract new buildings data
        new_buildings = self.current_data[self.current_data['composite_key'].isin(new_building_keys)].copy()
        new_buildings['change_type'] = 'new_building'
        
        # Find buildings that exist in both datasets
        common_keys = current_keys.intersection(previous_keys)
        logger.info(f"Found {len(common_keys)} buildings in both datasets")
        
        # Initialize empty dataframe for buildings with changes
        changes = pd.DataFrame(columns=self.current_data.columns)
        
        # Check for changes in name or location
        location_changes_detected = 0
        for key in common_keys:
            prev_row = self.previous_data[self.previous_data['composite_key'] == key].iloc[0]
            curr_row = self.current_data[self.current_data['composite_key'] == key].iloc[0]
            
            # Check for name changes
            name_changed = prev_row['name'] != curr_row['name'] and not pd.isna(prev_row['name']) and not pd.isna(curr_row['name'])
            
            # Check for location changes (based on lat/lon)
            loc_changed = False
            distance = 0
            
            if (not pd.isna(prev_row['lat']) and not pd.isna(curr_row['lat']) and 
                not pd.isna(prev_row['lon']) and not pd.isna(curr_row['lon'])):
                
                try:
                    # Calculate actual distance in meters using Haversine formula
                    distance = self.calculate_distance(
                        prev_row['lat'], prev_row['lon'],
                        curr_row['lat'], curr_row['lon']
                    )
                    
                    # Check if distance exceeds threshold (e.g., 300 meters)
                    loc_changed = distance > self.location_threshold
                    
                    if loc_changed:
                        location_changes_detected += 1
                        logger.info(f"Location change detected for {key}: {distance:.2f} meters")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error calculating distance for {key}: {e}")
            
            if name_changed or loc_changed:
                row_data = curr_row.copy()
                
                if name_changed and loc_changed:
                    row_data['change_type'] = 'name_and_location_change'
                    row_data['location_change_meters'] = distance
                    self.stats['name_changes'] += 1
                    self.stats['location_changes'] += 1
                elif name_changed:
                    row_data['change_type'] = 'name_change'
                    self.stats['name_changes'] += 1
                else:  # loc_changed
                    row_data['change_type'] = 'location_change'
                    row_data['location_change_meters'] = distance
                    self.stats['location_changes'] += 1
                
                # Add previous data values for comparison
                row_data['prev_name'] = prev_row['name']
                row_data['prev_lat'] = prev_row['lat']
                row_data['prev_lon'] = prev_row['lon']
                
                # Append to changes DataFrame
                changes = pd.concat([changes, pd.DataFrame([row_data])], ignore_index=True)
        
        # Combine new buildings and changes
        self.differences = pd.concat([new_buildings, changes], ignore_index=True)
        self.stats['total_changes'] = len(self.differences)
        
        logger.info(f"Total differences found: {self.stats['total_changes']}")
        logger.info(f"  - New buildings: {self.stats['new_buildings']}")
        logger.info(f"  - Name changes: {self.stats['name_changes']}")
        logger.info(f"  - Location changes: {self.stats['location_changes']} (threshold: {self.location_threshold} meters)")
        
        return self.differences
    
    def save_differences(self):
        """Save differences to CSV file"""
        if self.differences is None:
            raise ValueError("No differences to save. Run compare_datasets() first.")
        
        # Drop composite_key column which was only used for comparison
        if 'composite_key' in self.differences.columns:
            self.differences = self.differences.drop(columns=['composite_key'])
        
        # Save to CSV
        self.differences.to_csv(self.diff_output, index=False)
        logger.info(f"Differences saved to {self.diff_output}")
        
        return True
    
    def generate_report(self):
        """Generate a text report of the comparison results"""
        report = []
        report.append("=" * 50)
        report.append("ONEMAP BUILDING DATA COMPARISON REPORT")
        report.append("=" * 50)
        report.append(f"Previous dataset: {self.previous_file}")
        report.append(f"Current dataset: {self.current_file}")
        report.append(f"Differences output: {self.diff_output}")
        report.append("")
        report.append("STATISTICS")
        report.append("-" * 50)
        report.append(f"Previous dataset records: {len(self.previous_data)}")
        report.append(f"Current dataset records: {len(self.current_data)}")
        report.append(f"Total differences found: {self.stats['total_changes']}")
        report.append(f"  - New buildings: {self.stats['new_buildings']}")
        report.append(f"  - Name changes: {self.stats['name_changes']}")
        report.append(f"  - Location changes: {self.stats['location_changes']} (distance > {self.location_threshold} meters)")
        report.append("")
        report.append("SUMMARY")
        report.append("-" * 50)
        net_change = len(self.current_data) - len(self.previous_data)
        report.append(f"Net change in records: {net_change} ({net_change/len(self.previous_data)*100:.2f}%)")
        report.append("")
        report.append(f"Report generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 50)
        
        report_text = "\n".join(report)
        logger.info("\n" + report_text)
        
        # Save report to file
        report_filename = self.diff_output.replace(".csv", "_report.txt")
        with open(report_filename, "w") as f:
            f.write(report_text)
            
        logger.info(f"Report saved to {report_filename}")
        
        return report_text
    
    def run(self):
        """Run the entire comparison process"""
        self.load_data()
        self.compare_datasets()
        self.save_differences()
        self.generate_report()
        return self.differences

def main():
    """Main function to handle command line arguments and execute comparison"""
    parser = argparse.ArgumentParser(description='Compare two OneMap building datasets')
    parser.add_argument('--previous_file', type=str, required=True,
                        help='Path to previous OneMap dataset CSV file')
    parser.add_argument('--current_file', type=str, required=True,
                        help='Path to current OneMap dataset CSV file')
    parser.add_argument('--diff_output', type=str, default=None,
                        help='Path for output differences CSV file')
    parser.add_argument('--location_threshold', type=float, default=300.0,
                        help='Distance threshold in meters to detect location changes (default: 300)')
    
    args = parser.parse_args()
    
    # Create comparator instance
    comparator = OnemapComparator(
        args.previous_file, 
        args.current_file, 
        args.diff_output,
        args.location_threshold
    )
    
    # Run the comparison process
    comparator.run()

if __name__ == "__main__":
    main()