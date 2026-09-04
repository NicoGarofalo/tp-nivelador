package safe_socket

import (
	"io"
)

func SendAll(socket io.Writer, bytes []byte) error {
	total_bytes_sent := 0
	msg_size := len(bytes)
	for total_bytes_sent < msg_size {
		remaining_bytes := bytes[total_bytes_sent:]
		bytes_sent, err := socket.Write(remaining_bytes)
		if err != nil {
			return err
		}
		total_bytes_sent += bytes_sent
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	received_bytes := 0
	buff := make([]byte, size)
	for received_bytes < size {
		req_bytes := buff[received_bytes:]
		bytes_read, err := socket.Read(req_bytes)
		if err != nil {
			return nil, err
		}
		received_bytes += bytes_read
	}
	return buff[:received_bytes], nil
}
