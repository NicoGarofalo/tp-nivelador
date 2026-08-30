package safe_socket

import (
	"encoding/binary"
	"io"
)

const TWO_BYTES_SIZE = 2

// Ver despues de mover esto a protocol.go o similar
func SerializeNumber(number uint16) []byte {
	numberBuffer := make([]byte, TWO_BYTES_SIZE)
	binary.BigEndian.PutUint16(numberBuffer, number)
	return numberBuffer
}

// Ver despues de mover esto a protocol.go o similar
func DeserializeNumber(numberBuffer []byte) uint16 {
	return binary.BigEndian.Uint16(numberBuffer)
}

func SendAll(socket io.Writer, bytes []byte) error {
	// Envío el size del mensaje
	message_size := uint16(len(bytes))
	size_serialized := SerializeNumber(message_size)
	sendAll(socket, size_buffer)

	// Envío el mensaje
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
