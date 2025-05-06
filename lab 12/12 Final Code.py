from collections import defaultdict
import io
from google.colab import files
import pandas as pd
import os

# Function to process logs
def process_logs(log_data):
    # Dictionary to store counts of log levels
    log_count = defaultdict(int)
    
    # Process each line in the log data
    for line in log_data.strip().split('\n'):
        line = line.strip()
        
        # Assuming log level is the first word in each line
        if line:
            parts = line.split()
            if parts:  # Check if parts is not empty
                log_level = parts[0].upper()  # INFO, ERROR, WARN, etc.
                log_count[log_level] += 1
    
    # Return the results
    return log_count

def print_results(log_count):
    """Print the results in a formatted way"""
    print("\nLog Level Count Summary:")
    print("-----------------------")
    for log_level, count in log_count.items():
        print(f'{count}\t{log_level}')
    
    # Create a pandas DataFrame for better visualization
    df = pd.DataFrame(list(log_count.items()), columns=['Log Level', 'Count'])
    df = df.sort_values('Count', ascending=False)
    
    print("\nSorted by frequency:")
    print(df)
    
    # Return the dataframe for potential further analysis
    return df

# Sample log processing function
def process_sample():
    """Process a sample log for demonstration"""
    print("Processing sample log data...")
    
    # Sample log data
    sample_logs = """INFO Starting server
    ERROR Could not connect to database
    WARN Disk space low
    INFO Request completed
    ERROR User authentication failed
    DEBUG Connection attempt 1
    INFO Server shutdown
    CRITICAL System failure
    INFO Backup completed
    WARN Memory usage high"""
    
    # Process the logs
    log_results = process_logs(sample_logs)
    
    # Print and return results
    return print_results(log_results)

# Function to upload and process a log file
def upload_and_process_logs():
    """Upload a log file and process it"""
    print("Please upload your log file...")
    uploaded = files.upload()
    
    for filename in uploaded.keys():
        print(f"Processing file: {filename}")
        file_content = uploaded[filename].decode('utf-8')
        log_results = process_logs(file_content)
        print_results(log_results)
        
        # Save results to CSV if needed
        save_option = input("Do you want to save results to CSV? (y/n): ")
        if save_option.lower() == 'y':
            df = pd.DataFrame(list(log_results.items()), columns=['Log Level', 'Count'])
            df = df.sort_values('Count', ascending=False)
            result_filename = f"{os.path.splitext(filename)[0]}_analysis.csv"
            df.to_csv(result_filename, index=False)
            print(f"Results saved to {result_filename}")
            files.download(result_filename)

# NEW FUNCTION: Process pasted log content
def process_pasted_logs():
    """Process log data pasted directly by the user"""
    print("Please paste your log data below and press Enter twice when finished:")
    print("(Paste your log data, then press Enter twice to finish)")
    
    # Collect user input lines until they enter an empty line
    lines = []
    while True:
        try:
            line = input()
            if line.strip() == "":
                break
            lines.append(line)
        except EOFError:
            break
    
    log_data = "\n".join(lines)
    
    if not log_data.strip():
        print("No log data provided.")
        return None
    
    print(f"Processing {len(lines)} lines of pasted log data...")
    log_results = process_logs(log_data)
    df = print_results(log_results)
    
    # Save results to CSV if needed
    save_option = input("Do you want to save results to CSV? (y/n): ")
    if save_option.lower() == 'y':
        result_filename = "pasted_logs_analysis.csv"
        df.to_csv(result_filename, index=False)
        print(f"Results saved to {result_filename}")
        files.download(result_filename)
    
    return df

# Alternative paste method using a text area in Colab
def process_with_text_area():
    """Process log data using a text area for easier pasting"""
    from IPython.display import display
    import ipywidgets as widgets
    
    print("Use the text area below to paste your log data:")
    
    # Create a text area widget
    text_area = widgets.Textarea(
        value='',
        placeholder='Paste your log data here',
        description='Log Data:',
        disabled=False,
        layout=widgets.Layout(width='100%', height='300px')
    )
    
    display(text_area)
    
    # Create a button to process the data
    button = widgets.Button(description="Process Log Data")
    display(button)
    
    # Define the button click handler
    def on_button_clicked(b):
        log_data = text_area.value
        if not log_data.strip():
            print("No log data provided.")
            return
        
        print(f"Processing pasted log data...")
        log_results = process_logs(log_data)
        df = print_results(log_results)
        
        # Save results to CSV if needed
        save_button = widgets.Button(description="Save to CSV")
        display(save_button)
        
        def on_save_clicked(b):
            result_filename = "text_area_logs_analysis.csv"
            df.to_csv(result_filename, index=False)
            print(f"Results saved to {result_filename}")
            files.download(result_filename)
        
        save_button.on_click(on_save_clicked)
    
    button.on_click(on_button_clicked)

# Function to process logs from a mounted Google Drive
def process_drive_log(file_path):
    """Process a log file from Google Drive"""
    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None
    
    print(f"Processing file: {file_path}")
    with open(file_path, 'r') as file:
        log_data = file.read()
    
    log_results = process_logs(log_data)
    return print_results(log_results)

# Main function to provide options
def main():
    print("Log Processor for Google Colab")
    print("=============================")
    print("Please select an option:")
    print("1. Process sample log data")
    print("2. Upload and process a log file")
    print("3. Process a log file from Google Drive (requires mounting Drive first)")
    print("4. Paste log data directly")
    print("5. Use text area for pasting log data (recommended for larger logs)")
    
    choice = input("Enter your choice (1-5): ")
    
    if choice == '1':
        process_sample()
    elif choice == '2':
        upload_and_process_logs()
    elif choice == '3':
        # Guide user to mount drive if they haven't already
        if not os.path.exists('/content/drive'):
            print("You need to mount Google Drive first.")
            print("Run the following code in a cell:")
            print("from google.colab import drive")
            print("drive.mount('/content/drive')")
            return
        
        file_path = input("Enter the path to your log file in Google Drive: ")
        process_drive_log(file_path)
    elif choice == '4':
        process_pasted_logs()
    elif choice == '5':
        process_with_text_area()
    else:
        print("Invalid choice. Please run again and select options 1-5.")

# Example of direct use with pasted content (alternative approach)
def analyze_pasted_content(log_content):
    """Directly analyze the provided log content string"""
    print("Processing log content ({} lines)...".format(len(log_content.split('\n'))))
    log_results = process_logs(log_content)
    return print_results(log_results)

# Run the main function
if __name__ == "__main__":
    main()

# To use the direct analysis function, uncomment and replace with your log content:
"""
log_content = \"\"\"
INFO Starting server
ERROR Could not connect to database
WARN Disk space low
INFO Request completed
ERROR User authentication failed
\"\"\"
analyze_pasted_content(log_content)
"""