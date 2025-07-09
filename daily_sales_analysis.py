#!/usr/bin/env python3
"""
Daily Sales Analysis Script
Analyzes sales data for yesterday only (if it was a working day)

This script calculates:
- New Clients Closed
- New Clients Closed (Organic)
- Total New Clients Closed
- Total Rebuys
- New Client Revenue
- Rebuy Revenue
- Total Revenue
- Running Average Deal Size
- Appointments Booked/Conducted
- Daily Show Percentage
- Running Close Rate

For each sales representative for yesterday's data only.
"""

import sys
import os
from datetime import datetime

# Add the current directory to the path so we can import merged.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from merged import (
        analyze_sales_data_by_date, 
        save_daily_sales_metrics_to_csv,
        write_daily_data_to_master_sheet,
        create_sheet_name_for_date,
        should_run_analysis,
        get_yesterday_est
    )
    print("✅ Successfully imported functions from merged.py")
except ImportError as e:
    print(f"❌ Error importing from merged.py: {e}")
    print("Make sure merged.py is in the same directory and all dependencies are installed.")
    sys.exit(1)

def main():
    """Main function to run daily sales analysis for yesterday only"""
    print("🚀 Starting Daily Sales Analysis for Yesterday...")
    print("=" * 60)
    
    # Check if yesterday was a working day
    yesterday = get_yesterday_est()
    print(f"Target date: {yesterday}")
    
    if not should_run_analysis():
        print("Analysis skipped - yesterday was not a working day")
        return
    
    try:
        # Run the analysis
        result = analyze_sales_data_by_date()
        
        if result:
            print("\n✅ Analysis completed successfully!")
            
            # Save to CSV
            save_daily_sales_metrics_to_csv(result)
            
            # Write to master sheet
            master_sheet_success = write_daily_data_to_master_sheet(result)
            
            # Print summary info
            current_month = result.get('current_month', 'Unknown')
            current_year = result.get('current_year', 'Unknown')
            all_dates = result.get('all_dates', [])
            
            print(f"\n📊 ANALYSIS SUMMARY:")
            print("-" * 40)
            print(f"Period: {current_month}/{current_year}")
            print(f"Date Processed: {yesterday}")
            print(f"Representatives: Sierra, Mikaela, Mike")
            
            # Show team totals summary for yesterday
            daily_metrics = result.get('daily_metrics', {})
            if 'TEAM_TOTALS' in daily_metrics:
                team_totals = daily_metrics['TEAM_TOTALS']
                yesterday_str = yesterday.strftime('%Y-%m-%d')
                
                if yesterday_str in team_totals:
                    day_data = team_totals[yesterday_str]
                    
                    print(f"\n🎯 TEAM TOTALS FOR YESTERDAY ({yesterday}):")
                    print(f"New Clients Closed: {day_data['New Clients Closed']}")
                    print(f"New Clients Closed (Organic): {day_data['New Clients Closed (Organic)']}")
                    print(f"Total New Clients Closed: {day_data['Total New Clients Closed']}")
                    print(f"Total Rebuys: {day_data['Total Rebuys']}")
                    print(f"New Client Revenue: ${day_data['New Client Revenue']:,.2f}")
                    print(f"Rebuy Revenue: ${day_data['Rebuy Revenue']:,.2f}")
                    print(f"Total Revenue: ${day_data['Total Revenue']:,.2f}")
                    print(f"Running Average Deal Size: ${day_data['Average Deal Size']:,.2f}")
                    print(f"Appointments Booked: {day_data['Appointments Booked']}")
                    print(f"Appointments Conducted: {day_data['Appointments Conducted']}")
                    print(f"Daily Show Percentage: {day_data['Daily Show Percentage']:.1f}%")
                    print(f"Running Close Rate: {day_data['Running Close Rate']:.1f}%")
                else:
                    print(f"\n⚠️  No team totals data found for yesterday ({yesterday})")
            
            print(f"\nDaily metrics have been calculated for yesterday ({yesterday}).")
            print("Zero values are used for representatives with no sales data.")
            print("Running calculations include historical data from master sheet.")
            
            if master_sheet_success:
                print(f"✅ Data has been written to master sheet in sub-sheet: '{create_sheet_name_for_date(yesterday)}'")
            else:
                print(f"❌ Failed to write data to master sheet (see error above)")
                print(f"   CSV file still contains all the data: check the most recent daily_sales_metrics_*.csv file")
            
        else:
            print("❌ Analysis returned empty result")
            print("This could mean:")
            print("- No sales data found for yesterday")
            print("- Google Sheets configuration issue")
            print("- Data formatting issue")
            
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
    print(f"\n✅ Daily Sales Analysis completed for {get_yesterday_est()}!") 