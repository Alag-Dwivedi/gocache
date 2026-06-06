package protocol

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

// ParseRESPCommand reads exactly one command off the wire safely.
func ParseRESPCommand(reader *bufio.Reader) ([]string, error) {
	// 1. Read the first line to determine the type of request
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err // Connection closed or network error
	}
	line = strings.TrimSpace(line)

	if line == "" {
		return nil, errors.New("empty command")
	}

	// 2. If it doesn't start with '*', it's a raw text command (e.g. from telnet)
	if !strings.HasPrefix(line, "*") {
		return strings.Fields(line), nil
	}

	// 3. Parse the number of arguments in the RESP Array (e.g., "*3" means 3 args)
	numArgs, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	var args []string
	for i := 0; i < numArgs; i++ {
		// Read the string length header (e.g., "$3")
		lenLine, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lenLine = strings.TrimSpace(lenLine)

		if !strings.HasPrefix(lenLine, "$") {
			return nil, errors.New("expected bulk string indicator '$'")
		}

		strLen, err := strconv.Atoi(lenLine[1:])
		if err != nil {
			return nil, err
		}

		// Read the EXACT number of bytes for the string, plus 2 for the \r\n terminator.
		// Using io.ReadFull prevents the server from hanging waiting for newlines in binary data.
		buf := make([]byte, strLen+2)
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return nil, err
		}

		// Append the string (stripping off the trailing \r\n)
		args = append(args, string(buf[:strLen]))
	}

	return args, nil
}
