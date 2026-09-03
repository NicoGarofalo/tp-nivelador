package protocol

import (
	"encoding/binary"
	"net"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const LENGTH_MSG_BYTES_SIZE = 2
const CODE_BYTES_SIZE = 1

type Protocol struct {
	conn net.Conn
	sendBuffer []byte
}

const (
	internalHandshakeCode  byte = 0x01
	internalBatchBetCode   byte = 0x02
	internalBatchAck	   byte = 0x03
	internalWinnerCode     byte = 0x04
	internalEndBetsCode    byte = 0x05
	internalEndWinnersCode byte = 0x06
)


func NewProtocol(conn net.Conn) *Protocol {
	return &Protocol{conn: conn, sendBuffer: make([]byte, 0, 4096) }
}

func serializeNumber(number uint16) []byte {
	numberBuffer := make([]byte, LENGTH_MSG_BYTES_SIZE)
	binary.BigEndian.PutUint16(numberBuffer, number)
	return numberBuffer
}

func deserializeNumber(numberBuffer []byte) uint16 {
	return binary.BigEndian.Uint16(numberBuffer)
}

// ============================================
// Métodos SEND
// ============================================

func (p *Protocol) sendMessage(code byte, message []byte) error {
	clear(p.sendBuffer)
	p.sendBuffer = p.sendBuffer[:0]
	p.sendBuffer = append(p.sendBuffer, code)
	
	if len(message) > 0 {
		// Serializo tamaño del mensaje e inicializo el mensaje a enviar con el messageSize
		messageSize := uint16(len(message))
		p.sendBuffer = append(p.sendBuffer, serializeNumber(messageSize)...)
		// Appendeo el contenido del mensaje luego de su size.
		p.sendBuffer = append(p.sendBuffer, message...)
	}
	
	// Envío mensaje
	return safe_socket.SendAll(p.conn, p.sendBuffer)
}

func (p *Protocol) SendAgencyId(agencyId string) error {
	return p.sendMessage(internalHandshakeCode, []byte(agencyId))
}

func (p *Protocol) SendBetBatch(bet_batch []byte) error {
	if err := p.sendMessage(internalBatchBetCode, bet_batch); err != nil {
		return err
	}
	
	ackCode, err := p.recvCode()
	if err != nil {
		return err
	}
	// Aca no se bien como handlear. Como creo errores?
	if ackCode != internalBatchAck {
		return nil
	}
	return nil
}

func (p *Protocol) SendMessageBetsEnd()  error {
	return p.sendMessage(internalEndBetsCode, nil)
}

// ============================================
// Métodos RECV
// ============================================

func (p *Protocol) recvMessage() (string, error) {
	// Recibo size del mensaje
	buffSize, err := safe_socket.RecvAll(p.conn, LENGTH_MSG_BYTES_SIZE)
	if err != nil {
		return "", err
	}
	messageSize := deserializeNumber(buffSize)

	// Recibo mensaje
	buff, err := safe_socket.RecvAll(p.conn, int(messageSize))
	if err != nil {
		return "", err
	}
	return string(buff), nil
}

func (p *Protocol) recvCode() (byte, error) {
	code, err := safe_socket.RecvAll(p.conn, CODE_BYTES_SIZE)
	if err != nil {
		return 0, err
	}
	return code[0], nil
}

func (p *Protocol) RecvWinner() (string, error) {
	code, err := p.recvCode()
	if err != nil || (code != internalWinnerCode && code != internalEndWinnersCode) {
		return "", err
	}
	if code == internalEndWinnersCode {
		return "", nil
	}
	return p.recvMessage()
}
