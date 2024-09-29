package main

import (
	"bufio"
	"fmt"
	"github.com/redis/go-redis/v9"
	"golang.org/x/net/context"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Structure to hold data to send
type Data struct {
	SrNo      int
	Operator  string
	Num1      int
	Num2      int
	Answer    int
	Timestamp string
}

// Function to process and send data through the channel
func processData(data Data, conn net.Conn, correctCounter, incorrectCounter *int, mu *sync.Mutex, redisClient *redis.Client, ctx context.Context) {
	// Prepare the message to send
	message := fmt.Sprintf("%d,%s,%d,%d,%s\n", data.SrNo, data.Operator, data.Num1, data.Num2, data.Timestamp)

	// Send the message
	_, err := fmt.Fprint(conn, message)
	if err != nil {
		log.Println("Error sending data:", err)
		return
	}

	// Read response from the server
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		log.Println("Error reading response from server:", err)
		return
	}

	// Process response
	mu.Lock()
	defer mu.Unlock()

	responseParts := strings.Split(strings.TrimSpace(response), ",")
	if len(responseParts) != 2 {
		log.Println("Invalid response format from server:", response)
		return
	}

	// timestamp2 := responseParts[1]

	result, err := convertToInt(responseParts[0])
	if err != nil {
		log.Println("Error:", err)
	}

	// stat/us := "incorrect"
	if result == data.Answer {
		(*correctCounter)++
		// status = "correct"
	} else {
		(*incorrectCounter)++
	}

	// Store status and timestamp in Redis
	// recordKey := fmt.Sprintf("%d", data.SrNo)
	// err = redisClient.HSet(ctx, recordKey, "status", status).Err()
	// if err != nil {
	// 	log.Println("Error writing status to Redis:", err)
	// }
	// err = redisClient.HSet(ctx, recordKey, "Timestamp2", timestamp2).Err()
	// if err != nil {
	// 	log.Println("Error:", err)
	// }
}

// Worker function to process data from a specific channel
func worker(ch chan Data, conn net.Conn, correctCounter, incorrectCounter *int, mu *sync.Mutex, redisClient *redis.Client, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for data := range ch {
		processData(data, conn, correctCounter, incorrectCounter, mu, redisClient, ctx)
	}
}

// Helper function to convert string to int
func convertToInt(input string) (int, error) {
	if floatValue, err := strconv.ParseFloat(input, 64); err == nil {
		return int(floatValue), nil
	}
	return strconv.Atoi(input)
}

func temp(count int) int {
	// Redis setup
	ctx := context.Background()
	redisClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})

	redisClient2 := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   1,
	})

	var wg sync.WaitGroup
	var correctCounter, incorrectCounter int
	var mu sync.Mutex

	// Channels for each operation
	additionCh := make(chan Data, 100)
	subtractionCh := make(chan Data, 100)
	multiplicationCh := make(chan Data, 100)
	divisionCh := make(chan Data, 100)

	// Server connections
	additionConn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("Error connecting for addition:", err)
	}
	defer additionConn.Close()

	subtractionConn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("Error connecting for subtraction:", err)
	}
	defer subtractionConn.Close()

	multiplicationConn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("Error connecting for multiplication:", err)
	}
	defer multiplicationConn.Close()

	divisionConn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("Error connecting for division:", err)
	}
	defer divisionConn.Close()

	// Goroutines for each operation's worker
	wg.Add(4)
	go worker(additionCh, additionConn, &correctCounter, &incorrectCounter, &mu, redisClient2, ctx, &wg)
	go worker(subtractionCh, subtractionConn, &correctCounter, &incorrectCounter, &mu, redisClient2, ctx, &wg)
	go worker(multiplicationCh, multiplicationConn, &correctCounter, &incorrectCounter, &mu, redisClient2, ctx, &wg)
	go worker(divisionCh, divisionConn, &correctCounter, &incorrectCounter, &mu, redisClient2, ctx, &wg)

	// Fetch data from Redis and distribute to the appropriate channel
	for srNo := 1; srNo <= count; srNo++ {
		recordKey := fmt.Sprintf("%d", srNo)

		// Fetch the data from Redis
		result, err := redisClient.HGetAll(ctx, recordKey).Result()
		if err != nil {
			log.Println("Error fetching operator:", err)
			continue
		}

		num1Str := result["num1"]
		num2Str := result["num2"]
		operator := result["operator"]
		answerStr := result["answer"]

		num1, err := convertToInt(num1Str)
		if err != nil {
			log.Println("Error converting num1 to int:", err)
			continue
		}

		num2, err := convertToInt(num2Str)
		if err != nil {
			log.Println("Error converting num2 to int:", err)
			continue
		}

		answer, err := convertToInt(answerStr)
		if err != nil {
			log.Println("Error converting answer to int:", err)
			continue
		}

		data := Data{
			SrNo:      srNo,
			Operator:  operator,
			Num1:      num1,
			Num2:      num2,
			Answer:    answer,
			Timestamp: time.Now().Format(time.RFC3339),
		}

		// Send data to the corresponding channel
		switch operator {
		case "+":
			additionCh <- data
		case "-":
			subtractionCh <- data
		case "*":
			multiplicationCh <- data
		case "/":
			divisionCh <- data
		default:
			log.Printf("Unknown operator %s for record %d\n", operator, srNo)
		}
	}

	// Close channels after sending all data
	close(additionCh)
	close(subtractionCh)
	close(multiplicationCh)
	close(divisionCh)

	// Wait for all goroutines to finish
	wg.Wait()

	// Print the final results
	fmt.Printf("Correct answers: %d, Incorrect answers: %d\n", correctCounter, incorrectCounter)

	return correctCounter
}

func main() {
	for count := 100000; count <= 5000000; count += 100000 {
		nums := temp(count)

		if nums != count {
			break
		}

	}
}



//WITHOUT CHANNELS

// package main

// import (
// 	"bufio"
// 	"fmt"
// 	"github.com/redis/go-redis/v9"
// 	"golang.org/x/net/context"
// 	"log"
// 	"net"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"time"
// )

// // Structure to hold data to send
// type Data struct {
// 	SrNo      int
// 	Operator  string
// 	Num1      int
// 	Num2      int
// 	Answer    int
// 	Timestamp string
// }

// // Function to send data to the server and receive the response
// func sendDataToServer(conn net.Conn, data Data, correctCounter, incorrectCounter *int, mu *sync.Mutex, redisClient *redis.Client, ctx context.Context) {
// 	// Prepare the message to send
// 	message := fmt.Sprintf("%d,%s,%d,%d,%s\n", data.SrNo, data.Operator, data.Num1, data.Num2, data.Timestamp)

// 	// Send the message
// 	_, err := fmt.Fprint(conn, message)
// 	if err != nil {
// 		log.Println("Error sending data:", err)
// 		return
// 	}

// 	// Read response (calculated answer and timestamp) from server
// 	response, err := bufio.NewReader(conn).ReadString('\n')
// 	if err != nil {
// 		log.Println("Error reading response from server:", err)
// 		return
// 	}

// 	// Check if the received answer matches the expected answer
// 	mu.Lock() // Lock the mutex before updating the counters
// 	defer mu.Unlock()

// 	responseParts := strings.Split(strings.TrimSpace(response), ",")
// 	if len(responseParts) != 2 {
// 		log.Println("Invalid response format from server:", response)
// 		return
// 	}

// 	timestamp2 := responseParts[1]

// 	result, err := convertToInt(responseParts[0])
// 	if err != nil {
// 		log.Println("Error: ", err)
// 	}

// 	status := "incorrect"
// 	if result == data.Answer {
// 		(*correctCounter)++
// 		status = "correct"
// 	} else {
// 		(*incorrectCounter)++
// 	}

// 	// Write the status to Redis based on SrNo
// 	recordKey := fmt.Sprintf("%d", data.SrNo)
// 	err = redisClient.HSet(ctx, recordKey, "status", status).Err()
// 	if err != nil {
// 		log.Println("Error writing status to Redis:", err)
// 	}
// 	err = redisClient.HSet(ctx, recordKey, "Timestamp2", timestamp2).Err()
// 	if err != nil {
// 		log.Println("Error: ", err)
// 	}
// }

// // Function to handle an operation using a given connection
// func handleOperation(data Data, wg *sync.WaitGroup, conn net.Conn, correctCounter, incorrectCounter *int, mu *sync.Mutex, redisClient *redis.Client, ctx context.Context) {
// 	defer wg.Done()
// 	sendDataToServer(conn, data, correctCounter, incorrectCounter, mu, redisClient, ctx)
// }

// // Helper function to convert string to int, handling float inputs as well
// func convertToInt(input string) (int, error) {
// 	// Try converting to float first, then to int
// 	if floatValue, err := strconv.ParseFloat(input, 64); err == nil {
// 		return int(floatValue), nil
// 	}
// 	return strconv.Atoi(input) // Fallback to regular Atoi
// }

// func temp(count int) int {
// 	// Step 1: Connect to Redis
// 	ctx := context.Background()
// 	redisClient := redis.NewClient(&redis.Options{
// 		Addr: "127.0.0.1:6379", // Redis server address
// 		DB:   0,                // Default DB
// 	})

// 	redisClient2 := redis.NewClient(&redis.Options{
// 		Addr: "127.0.0.1:6379", // Redis server address
// 		DB:   1,                // Default DB
// 	})

// 	var wg sync.WaitGroup
// 	var correctCounter, incorrectCounter int
// 	var mu sync.Mutex // Mutex for synchronizing access to counters

// 	correctCounter = 0
// 	incorrectCounter = 0

// 	// Step 2: Create connections for each operation type (reinitialize for each iteration)
// 	additionConn, err := net.Dial("tcp", "localhost:8080")
// 	if err != nil {
// 		log.Fatal("Error connecting for addition:", err)
// 	}

// 	subtractionConn, err := net.Dial("tcp", "localhost:8080")
// 	if err != nil {
// 		log.Fatal("Error connecting for subtraction:", err)
// 	}

// 	multiplicationConn, err := net.Dial("tcp", "localhost:8080")
// 	if err != nil {
// 		log.Fatal("Error connecting for multiplication:", err)
// 	}

// 	divisionConn, err := net.Dial("tcp", "localhost:8080")
// 	if err != nil {
// 		log.Fatal("Error connecting for division:", err)
// 	}

// 	for srNo := 1; srNo <= count; srNo++ {
// 		recordKey := fmt.Sprintf("%d", srNo)

// 		// Fetch the data from Redis
// 		result, err := redisClient.HGetAll(ctx, recordKey).Result()
// 		if err != nil {
// 			log.Println("Error fetching operator:", err)
// 			continue
// 		}

// 		num1Str := result["num1"]
// 		num2Str := result["num2"]
// 		operator := result["operator"]
// 		answerStr := result["answer"]

// 		// Convert string values to integers with error checking
// 		num1, err := convertToInt(num1Str)
// 		if err != nil {
// 			log.Println("Error converting num1 to int:", err)
// 			continue
// 		}

// 		num2, err := convertToInt(num2Str)
// 		if err != nil {
// 			log.Println("Error converting num2 to int:", err)
// 			continue
// 		}

// 		answer, err := convertToInt(answerStr)
// 		if err != nil {
// 			log.Println("Error converting answer to int:", err)
// 			continue
// 		}

// 		// Prepare data to send
// 		data := Data{
// 			SrNo:      srNo,
// 			Operator:  operator,
// 			Num1:      num1,
// 			Num2:      num2,
// 			Answer:    answer,
// 			Timestamp: time.Now().Format(time.RFC3339), // Current timestamp
// 		}

// 		// Step 4: Use goroutines based on the operator
// 		wg.Add(1) // Add to the wait group for each goroutine
// 		switch operator {
// 		case "+":
// 			go handleOperation(data, &wg, additionConn, &correctCounter, &incorrectCounter, &mu, redisClient2, ctx)
// 		case "-":
// 			go handleOperation(data, &wg, subtractionConn, &correctCounter, &incorrectCounter, &mu, redisClient2, ctx)
// 		case "*":
// 			go handleOperation(data, &wg, multiplicationConn, &correctCounter, &incorrectCounter, &mu, redisClient2, ctx)
// 		case "/":
// 			go handleOperation(data, &wg, divisionConn, &correctCounter, &incorrectCounter, &mu, redisClient2, ctx)
// 		default:
// 			log.Printf("Unknown operator %s for record %d\n", operator, srNo)
// 			wg.Done() // Mark as done if the operator is unknown
// 		}
// 	}

// 	wg.Wait()

// 	// Step 6: Print results and check failure condition
// 	fmt.Printf("Correct answers: %d, Incorrect answers: %d\n", correctCounter, incorrectCounter)

// 	// Close connections for the current iteration
// 	defer additionConn.Close()
// 	defer subtractionConn.Close()
// 	defer multiplicationConn.Close()
// 	defer divisionConn.Close()

// 	return correctCounter
// }

// func main() {
// 	for count := 100; count <= 5000000; count += 100 {
// 		nums := temp(count)

// 		if nums != count {
// 			break
// 		}

// 		time.Sleep(1 * time.Second)
// 	}
// }

