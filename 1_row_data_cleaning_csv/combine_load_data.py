import pandas as pd
import os

def process_weather_data(input_filename='weather_20210401-20250930.csv', 
                         output_filename='processed_weather_data.csv'):
    """
    Processes the weather data file: combines date and time columns, 
    and keeps only the specified columns.
    """
    
    # Check if the input file exists
    if not os.path.exists(input_filename):
        print(f"ERROR: File not found '{input_filename}'. Please ensure the file is in the same directory as the script.")
        return

    print(f"Reading file: {input_filename}...")
    
    try:
        # 1. Read the CSV file
        df = pd.read_csv(input_filename)
        
        # ----------------------------------------------------
        # 2. Create the datetime column (datetime_ept)
        # ----------------------------------------------------
        
        # Ensure MO, DY, HR columns are strings and zero-padded to two digits
        df['MO'] = df['MO'].astype(str).str.zfill(2)
        df['DY'] = df['DY'].astype(str).str.zfill(2)
        df['HR'] = df['HR'].astype(str).str.zfill(2)
        
        # Combine YEAR, MO, DY, HR columns
        # Format is 'YYYY-MM-DD HH:MM:SS'
        df['datetime_str'] = (
            df['YEAR'].astype(str) + '-' +
            df['MO'] + '-' +
            df['DY'] + ' ' +
            df['HR'] + ':00:00'
        )
        
        # Convert to datetime object
        df['datetime_ept'] = pd.to_datetime(df['datetime_str'])
        
        # ----------------------------------------------------
        # 3. Select and Reorder Columns
        # ----------------------------------------------------
        
        # Define the final columns to keep
        final_columns = [
            'datetime_ept',
            'TEMP', 
            'PRCP', 
            'HMDT', 
            'WND_SPD', 
            'ATM_PRESS'
        ]
        
        # Check if all final columns exist in the original data
        missing_cols = [col for col in final_columns if col not in df.columns and col != 'datetime_ept']
        if missing_cols:
            print(f"WARNING: The following columns are missing from the original file: {missing_cols}. Please check column names.")
            # If missing, select only the existing columns
            final_columns = [col for col in final_columns if col in df.columns or col == 'datetime_ept']
            
        df_final = df[final_columns].copy()
        
        # Ensure data is correctly sorted by time
        df_final = df_final.sort_values(by='datetime_ept').reset_index(drop=True)

        # ----------------------------------------------------
        # 4. Save Results
        # ----------------------------------------------------
        
        df_final.to_csv(output_filename, index=False)
        
        print("\n----------------------------------------------------")
        print(f"✅ Processing Complete!")
        print(f"Total Records: {len(df_final)} rows")
        print(f"Results saved to file: {os.path.abspath(output_filename)}")
        print("----------------------------------------------------")
        
    except Exception as e:
        print(f"An error occurred during processing: {e}")

if __name__ == "__main__":
    process_weather_data()