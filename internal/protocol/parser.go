package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ParseRESPCommand reads a raw incoming RESP payload stream and extracts the string array arguments.
func ParseRESPCommand(reader *bufio.Reader) ([]string, error) {
	// 1. Read the array indicator prefix byte (e.g. '*')
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	if prefix != '*' {
		// Treat line as a inline plain text fallback command if it doesn't start with '*'
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		return strings.Fields(strings.TrimSpace(string(prefix) + line)), nil
	}

	// 2. Read how many elements are inside the array
	countStr, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(countStr))
	if err != nil || count <= 0 {
		return nil, errors.New("invalid array length descriptor")
	}

	args := make([]string, count)

	// 3. Loop and parse each Bulk String entry sequentially
	for i := range count {
		dollar, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if dollar != '$' {
			return nil, fmt.Errorf("expected bulk string indicator '$', got '%c'", dollar)
		}

		// Read bulk string length
		lenStr, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		strLen, err := strconv.Atoi(strings.TrimSpace(lenStr))
		if err != nil {
			return nil, errors.New("invalid bulk string size header")
		}

		// Read the actual body payload bytes
		buf := make([]byte, strLen)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf)

		// Read and discard trailing \r\n padding bytes
		if _, err := reader.ReadString('\n'); err != nil {
			return nil, err
		}
	}

	return args, nil
}
