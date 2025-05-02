# Define MapReduce functions directly in the notebook
import re
from collections import defaultdict
import os

def mapper(input_lines):
    mapped_data = []
    for line in input_lines:
        line = line.strip().lower()
        line = re.sub(r'[^\w\s]', ' ', line)
        words = line.split()
        
        for word in words:
            if word:
                mapped_data.append((word, 1))
    
    return mapped_data

def reducer(mapped_data):
    word_counts = defaultdict(int)
    
    for word, count in mapped_data:
        word_counts[word] += count
    
    return word_counts

def mapreduce_word_count(file_path):
    # Check if file exists
    if not os.path.exists(file_path):
        return f"Error: File '{file_path}' not found."
    
    # Read file
    with open(file_path, 'r') as f:
        input_lines = f.readlines()
    
    # Map phase
    mapped_data = mapper(input_lines)
    
    # Sort phase
    mapped_data.sort(key=lambda x: x[0])
    
    # Reduce phase
    result = reducer(mapped_data)
    
    # Sort results by count (descending)
    sorted_results = sorted(result.items(), key=lambda x: x[1], reverse=True)
    
    # Format and return results
    return '\n'.join([f"{count}\t{word}" for word, count in sorted_results])

# Use the MapReduce function directly on your file
# Make sure "New Text Document.txt" is uploaded to your Colab environment
print("Word count results for 'New Text Document.txt':")
results = mapreduce_word_count('New Text Document.txt')
print(results)