import pandas as pd
from datetime import datetime
import os
import numpy as np

# Define file paths
file_paths = [
    './data/id1421091911_non-organic-in-app-events_2025-04-14_2025-04-20_Asia_Singapore.csv',
    './data/com.dreame.reader_non-organic-in-app-events_2025-04-14_2025-04-20_Asia_Singapore.csv'
]

df = pd.DataFrame()
# Check if files exist
for file_path in file_paths:
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found")
        exit(1)
    
    # Read the CSV files
    print(f"Reading file: {file_path}")
    single_df = pd.read_csv(file_path)
    print(f"Total records in file: {len(single_df)}")

    df = pd.concat([df, single_df], ignore_index=True)

# Merge the dataframes
print("After merging data files...")
print(f"Total records after merge: {len(df)}")

# Convert time columns to datetime
df['Install Time'] = pd.to_datetime(df['Install Time'])
df['Event Time'] = pd.to_datetime(df['Event Time'])

# Calculate time difference in hours
df['Time Difference (hours)'] = (df['Event Time'] - df['Install Time']).dt.total_seconds() / 3600

# Define time periods in hours
time_periods = {
    '1 minute': 1/60,  # 1 minute in hours
    '1 day': 24,
    '3 days': 72,
    '7 days': 168,
    '30 days': 720
}

print("\n====== ALL DATA ANALYSIS ======")

# Calculate total events and revenue
total_events = len(df)
total_revenue = df['Event Revenue USD'].sum()

print(f"\nTotal number of events in dataset: {total_events}")
print(f"Total revenue from all events: ${total_revenue:.2f} USD")
print("\n=== Time period statistics ===")

# Calculate statistics for each time period
for period_name, hours in time_periods.items():
    # Filter events for this time period
    period_events = df[df['Time Difference (hours)'] <= hours]
    
    # Calculate event counts and percentages
    period_event_count = len(period_events)
    period_event_percentage = (period_event_count / total_events) * 100 if total_events > 0 else 0
    
    # Calculate revenue and percentages
    period_revenue = period_events['Event Revenue USD'].sum()
    period_revenue_percentage = (period_revenue / total_revenue) * 100 if total_revenue > 0 else 0
    
    # Print statistics for this period
    print(f"\n{period_name}:")
    print(f"  Events: {period_event_count} / {total_events} ({period_event_percentage:.2f}%)")
    print(f"  Revenue: ${period_revenue:.2f} / ${total_revenue:.2f} ({period_revenue_percentage:.2f}%)")

# Calculate revenue percentile time differences
print("\n=== Revenue Percentile Analysis ===")

# Sort by time difference
df_sorted = df.sort_values('Time Difference (hours)')

# Calculate cumulative sum of revenue
df_sorted['Cumulative Revenue'] = df_sorted['Event Revenue USD'].cumsum()
df_sorted['Revenue Percentage'] = (df_sorted['Cumulative Revenue'] / total_revenue) * 100

# Find the time differences at specific revenue percentiles
percentiles = [50, 75, 90]
for percentile in percentiles:
    # Find the first row where we exceed the percentile
    row = df_sorted[df_sorted['Revenue Percentage'] >= percentile].iloc[0]
    hours = row['Time Difference (hours)']
    days = hours / 24
    minutes = hours * 60
    
    print(f"\nP{percentile} revenue reached at:")
    print(f"  Time difference: {hours:.2f} hours ({days:.2f} days or {minutes:.2f} minutes)")
    print(f"  Cumulative revenue: ${row['Cumulative Revenue']:.2f} ({row['Revenue Percentage']:.2f}%)")

# Filter data for installs between 2025-04-14 and 2025-04-20
start_date = '2025-04-14'
end_date = '2025-04-20 23:59:59'
filtered_df = df[(df['Install Time'] >= start_date) & (df['Install Time'] <= end_date)]

print("\n\n====== ANALYSIS FOR INSTALLS BETWEEN 2025-04-14 AND 2025-04-20 ======")

# Calculate total events and revenue for filtered data
filtered_total_events = len(filtered_df)
filtered_total_revenue = filtered_df['Event Revenue USD'].sum()

print(f"\nNumber of events with installs in date range: {filtered_total_events}")
print(f"Revenue from events with installs in date range: ${filtered_total_revenue:.2f} USD")
print(f"Percentage of total events: {(filtered_total_events / total_events) * 100:.2f}%")
print(f"Percentage of total revenue: {(filtered_total_revenue / total_revenue) * 100:.2f}%")
print("\n=== Time period statistics for filtered data ===")

# Calculate statistics for each time period with filtered data
for period_name, hours in time_periods.items():
    # Filter events for this time period
    period_events = filtered_df[filtered_df['Time Difference (hours)'] <= hours]
    
    # Calculate event counts and percentages
    period_event_count = len(period_events)
    period_event_percentage = (period_event_count / filtered_total_events) * 100 if filtered_total_events > 0 else 0
    
    # Calculate revenue and percentages
    period_revenue = period_events['Event Revenue USD'].sum()
    period_revenue_percentage = (period_revenue / filtered_total_revenue) * 100 if filtered_total_revenue > 0 else 0
    
    # Print statistics for this period
    print(f"\n{period_name}:")
    print(f"  Events: {period_event_count} / {filtered_total_events} ({period_event_percentage:.2f}%)")
    print(f"  Revenue: ${period_revenue:.2f} / ${filtered_total_revenue:.2f} ({period_revenue_percentage:.2f}%)")

# Calculate revenue percentile time differences for filtered data
if not filtered_df.empty:
    print("\n=== Revenue Percentile Analysis for Filtered Data ===")
    
    # Sort by time difference
    filtered_sorted = filtered_df.sort_values('Time Difference (hours)')
    
    # Calculate cumulative sum of revenue
    filtered_sorted['Cumulative Revenue'] = filtered_sorted['Event Revenue USD'].cumsum()
    filtered_sorted['Revenue Percentage'] = (filtered_sorted['Cumulative Revenue'] / filtered_total_revenue) * 100
    
    # Find the time differences at specific revenue percentiles
    for percentile in percentiles:
        # Find the first row where we exceed the percentile
        if len(filtered_sorted[filtered_sorted['Revenue Percentage'] >= percentile]) > 0:
            row = filtered_sorted[filtered_sorted['Revenue Percentage'] >= percentile].iloc[0]
            hours = row['Time Difference (hours)']
            days = hours / 24
            minutes = hours * 60
            
            print(f"\nP{percentile} revenue reached at:")
            print(f"  Time difference: {hours:.2f} hours ({days:.2f} days or {minutes:.2f} minutes)")
            print(f"  Cumulative revenue: ${row['Cumulative Revenue']:.2f} ({row['Revenue Percentage']:.2f}%)")
        else:
            print(f"\nP{percentile} revenue not reached in the filtered dataset")

# Display sample data from merged dataset
print("\nSample data from merged dataset:")
sample_data = df[['Install Time', 'Event Time', 'Time Difference (hours)', 'Event Revenue USD']].head(5)
print(sample_data) 