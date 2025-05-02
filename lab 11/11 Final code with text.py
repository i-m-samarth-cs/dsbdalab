# Define MapReduce functions directly in the notebook
import re
from collections import defaultdict

def mapper(input_text):
    mapped_data = []
    lines = input_text.split('\n')
    
    for line in lines:
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

def mapreduce_word_count(input_text):
    # Map phase
    mapped_data = mapper(input_text)
    
    # Sort phase
    mapped_data.sort(key=lambda x: x[0])
    
    # Reduce phase
    result = reducer(mapped_data)
    
    # Sort results by count (descending)
    sorted_results = sorted(result.items(), key=lambda x: x[1], reverse=True)
    
    # Format and return results
    return '\n'.join([f"{count}\t{word}" for word, count in sorted_results])

# Input your text directly here
input_text = """Enter your text here.
You can paste multiple lines.
The MapReduce algorithm will count all words."""

# Replace the above text with your actual content

# Process the input text
print("Word count results:")
results = mapreduce_word_count(input_text)
print(results)