package main

import (
    "bufio"
    "fmt"
    "log"
    "net"
    "strconv"
    "strings"
    "time"
)

// Define a constant for the buffer size
const BUFFER_SIZE = 2048

// handleConnection manages a single client connection
func handleConnection(conn net.Conn) {
    defer conn.Close() // Ensure the connection is closed when done

    // Create a scanner to read data from the connection
    scanner := bufio.NewScanner(conn)
    // Set the buffer for the scanner
    scanner.Buffer(make([]byte, BUFFER_SIZE), BUFFER_SIZE)

    // Read the incoming data line by line
    for scanner.Scan() {
        message := scanner.Text() // Get the text input from the scanner
        // Process the message received from the client
        response := processMessage(message)

        // Send the calculated answer back to the client
        if response != "" {
            _, err := conn.Write([]byte(response + "\n")) // Send response with a newline
            if err != nil {
                log.Println("Error sending response to client:", err)
            }
        }else {
            _, err := conn.Write([]byte("error" + "\n")) // Send response with a newline
            if err != nil {
                log.Println("Error sending response to client:", err)
            }
        }
    }

    // Check for any scanning errors
    if err := scanner.Err(); err != nil {
        log.Println("Error reading from connection:", err)
    }
}

// processMessage processes the incoming message and calculates the expected answer
func processMessage(message string) string {
    // Split the message by commas
    parts := strings.Split(message, ",")

    // Ensure the message has exactly 6 parts (SrNo, Operator, Num1, Num2, Answer, Timestamp)
    if len(parts) != 5 {
        log.Println("Invalid message format:", message)
        return ""
    }

    operator := parts[1]
    num1Str := parts[2]
    num2Str := parts[3]
    // answerStr := parts[4]

    // Convert string values to integers with error checking
    num1Int, err := strconv.Atoi(num1Str)
    if err != nil {
        log.Println("Error converting num1 to int:", err)
        return ""
    }

    num2Int, err := strconv.Atoi(num2Str)
    if err != nil {
        log.Println("Error converting num2 to int:", err)
        return ""
    }

    var expectedAnswer int
    // Calculate the expected answer based on the operator
    switch operator {
    case "+":
        expectedAnswer = num1Int + num2Int
    case "-":
        expectedAnswer = num1Int - num2Int
    case "*":
        expectedAnswer = num1Int * num2Int
    case "/":
        expectedAnswer = num1Int / num2Int
    default:
        log.Printf("Unknown operator: %s\n", operator)
        return ""
    }

    // Get the current timestamp
    timestamp := time.Now().Format(time.RFC3339)

    // Return the expected answer along with the timestamp as a string to be sent back to the client
    return fmt.Sprintf("%d, %s",expectedAnswer, timestamp)
}

func main() {
    // Start the server
    ln, err := net.Listen("tcp", ":8080")
    if err != nil {
        log.Fatal("Error starting server:", err)
    }
    defer ln.Close() // Close listener when done

    log.Println("Server is listening on port 8080...")
    for {
        conn, err := ln.Accept() // Accept new connections
        if err != nil {
            log.Println("Error accepting connection:", err)
            continue
        }

        // Handle the connection in a new goroutine
        go handleConnection(conn)
    }
}
