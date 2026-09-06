package protocol

import (
	"encoding/binary"
	"net"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const LENGTH_MSG_BYTES_SIZE = 2
const CODE_BYTES_SIZE = 1
const OFFSET_MSG_LEN_START = 1
const OFFSET_MSG_LEN_END = 3

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

// Reservo el espacio del bufer para enviar, como la suma del espacio del batch 
// sumado al espacio que ocupa el codigo de mensaje (1 byte) y
// al espacio que ocupa el tamaño del mensaje (2 bytes) para evitar realocaciones.
func NewProtocol(conn net.Conn, betBufferSize int) *Protocol {
	bufferSize := betBufferSize + CODE_BYTES_SIZE + LENGTH_MSG_BYTES_SIZE
	return &Protocol{conn: conn, sendBuffer: make([]byte, 0, bufferSize) }
}

// Función que serializa el número de int a byte en big endian para envio en socket.
func serializeNumber(dest []byte, number uint16) {
	binary.BigEndian.PutUint16(dest, number)
}

// Función que deserializa el número de []byte de big endian a int interno
func deserializeNumber(numberBuffer []byte) uint16 {
	return binary.BigEndian.Uint16(numberBuffer)
}

// ============================================
// Métodos SEND
// ============================================

// Función que serializa y envía un mensaje.
func (p *Protocol) sendMessage(code byte, message []byte) error {
	// Limpio el buffer
	clear(p.sendBuffer)
	p.sendBuffer = p.sendBuffer[:0]

	p.sendBuffer = append(p.sendBuffer, code)
	
	// Si me llega el mensaje, serializo su tamaño y lo agrego al buffer.
	if len(message) > 0 {
		// Serializo tamaño del mensaje e inicializo el mensaje a enviar con el messageSize
		messageSize := uint16(len(message))
		var numberBuffer [LENGTH_MSG_BYTES_SIZE]byte
		serializeNumber(numberBuffer[:], messageSize)

		p.sendBuffer = append(p.sendBuffer, numberBuffer[:]...)
		// Agrego el contenido del mensaje luego de su size
		p.sendBuffer = append(p.sendBuffer, message...)
	}
	
	// Envío mensaje
	return safe_socket.SendAll(p.conn, p.sendBuffer)
}

// Función que envía el agency id en bytes.
func (p *Protocol) SendAgencyId(agencyId string) error {
	return p.sendMessage(internalHandshakeCode, []byte(agencyId))
}

// Función que envía el batch de bets.
func (p *Protocol) SendBetBatch(bet_batch []byte) error {
	if err := p.sendMessage(internalBatchBetCode, bet_batch); err != nil {
		return err
	}
	
	ackCode, err := p.recvCode()
	if err != nil {
		return err
	}
	if ackCode != internalBatchAck {
		return err
	}
	return nil
}

// Función que envía el byte que indica que ya no se van a enviar mas batches de bets.
func (p *Protocol) SendMessageBetsEnd()  error {
	return p.sendMessage(internalEndBetsCode, nil)
}

// ============================================
// Métodos RECV
// ============================================

// Función general que recibe MSG_SIZE | MESSAGE.
func (p *Protocol) recvMessage() (string, error) {
	// Recibo size del mensaje y deserializo.
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

// Función que recibe el byte del código por socket. retorna el código recibido en caso de éxito.
func (p *Protocol) recvCode() (byte, error) {
	code, err := safe_socket.RecvAll(p.conn, CODE_BYTES_SIZE)
	if err != nil {
		return 0, err
	}
	return code[0], nil
}

// Función que recibe de a una bet ganadora. 
// Si ya no hay ganador porque se recibió el codigo de fin, retorna un string vacío.
// En caso de no recibir ganador, o recibir un codigo distinto a los esperados, retorna error.
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
