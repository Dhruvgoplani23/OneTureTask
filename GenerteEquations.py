import csv
import random

# Define available mathematical operators
Operators = ['+', '-', '*', '/']

def get_random_operator():
    """
    Return a random operator from the available operators.
    """
    return random.choice(Operators)

def generate_numbers():
    #Generate two random integers to be used in the mathematical operation.

    number1 = random.randint(1, 100)  # Random integer between 1 and 100
    number2 = random.randint(1, 100)  # Random integer between 1 and 100
    return number1, number2

def calculate_result(num1, num2, operator):
    """
    Calculate the result of a mathematical operation between two numbers.
    """
    if operator == '+':
        return num1 + num2
    elif operator == '-':
        return num1 - num2
    elif operator == '*':
        return num1 * num2
    elif operator == '/' and num2 != 0:  # Avoid division by zero
        return num1 / num2
    else:
        return None

def generate_row(index):
    """
    Generate a single row with:
    - Sr No
    - Random mathematical operator
    - Random Number 1
    - Random Number 2
    - Result of the operation
    """
    operator = get_random_operator()
    num1, num2 = generate_numbers()
    result = calculate_result(num1, num2, operator)
    return [index, operator, num1, num2, result]

def write_csv(filename, total_rows):
    #Write the CSV file with the given number of rows.

    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write the header row
        writer.writerow(['Sr No', 'Operator', 'Number 1', 'Number 2', 'Result'])
        
        # Generate and write each row
        for i in range(1, total_rows + 1):
            row = generate_row(i)
            writer.writerow(row)

if __name__ == "__main__":
    # Specify the output file and number of records to generate
    output_file = "large_dataset.csv"
    total_records = 5000000  # 50 lakh records
    
    # Generate the CSV file
    write_csv(output_file, total_records)
    print(f"CSV file '{output_file}' created successfully!")
