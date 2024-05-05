import csv
from datetime import datetime
import random


def generate_csv(filename, num_rows):
  
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write headers
        headers = ["id", "col1", "col2", "col3", "col4", "col5", "created_at"]
        writer.writerow(headers)
        
        # Write rows
        letters = ["a", "b", "c", "d", "e", "f", "g"]
        word_length = 10
        for _ in range(num_rows):

            
            row = [random.randint(1, 100)]
            for c in range(5):
              word = ""
              for l in range(word_length):
                word = word + random.choice(letters)
              row.append(word)
            
            current_time = datetime.now()
            # Format current time as a string
            current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
            row.append(current_time_str)
            
            writer.writerow(row)
            
if __name__ == "__main__":
  # generate_csv("../wings-data/large_data/data.csv", 1000000)
  generate_csv("../wings-data/single_medium_data/data.csv", 100000)
  # for i in range(100000):
  #   generate_csv(f"../wings-data/many_data/data_{i}.csv", 10)
  # for i in range(100):
  #   generate_csv(f"../wings-data/medium_data/data_{i}.csv", 1000)