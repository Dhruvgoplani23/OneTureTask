import csv
import redis

# Step 1: Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=1)

print("Connection successful")

print("Starting the writing part")

# Step 2: Open the CSV file and read data
with open('large_dataset.csv', 'r') as csvfile:
    csv_reader = csv.reader(csvfile)
    
    # Skip the header if it exists
    next(csv_reader)
    
    # Step 3: Iterate through the rows and store in Redis
    for row in csv_reader:
        sr_no = row[0]  # Unique identifier
        operator = row[1]
        num1 = row[2]
        num2 = row[3]
        answer = row[4]
        
        # Store each row as a Redis hash where the key is 'record:<sr_no>'
        redis_client.hset(sr_no, mapping={
            'operator': operator,
            'num1': num1,
            'num2': num2,
            'answer': answer
        })

print("Data loaded into Redis successfully!")
