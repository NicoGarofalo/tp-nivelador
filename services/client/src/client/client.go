package client

import (
	"bufio"
	"net"
	"os"
	"time"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200
const FILE_WRITE_PERMS = 0644

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize int
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

// Función encargada de enviar los batches de las bets.
// Recibe el protocolo del cliente para enviar los mensajes al server.
func (client *Client) sendBets(clientProtocol *protocol.Protocol) error {
	const mainAction = "test-send-bet"
	const estimatedBetSize = 128
	betBufferSize := estimatedBetSize * client.config.BatchSize

	messageLog := []any{"agency-id", client.config.AgencyId}

	// Abro archivo input-x.csv
	file, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("input-file-open", logger.Fail, messageLog...)
		return err
	}
	defer file.Close() // Para que ejecute el close cuando sale del scope de la función

	scanner := bufio.NewScanner(file)

	// Envío agency id (mensaje handshake) para comenzar comunicacion
	if err := clientProtocol.SendAgencyId(client.config.AgencyId); err != nil {
		logger.Error("send-agency-id", logger.Fail, messageLog...)
		return err
	}
	logger.Info("send-agency-id", logger.Success, messageLog...)

	betBatch := make([]byte, 0, betBufferSize)
	agencyIdInBytes := []byte(client.config.AgencyId)
	betsCount := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		sizeNeeded := len(agencyIdInBytes) + len(line) + 2
		
		// Corroboro si el bet que acabo de leer entra en el batch o si está lleno.
		// Si pasa alguna de las 2, envío lo que tengo y limpio el batch.
		if len(betBatch) > 0 && (len(betBatch) + sizeNeeded > betBufferSize || betsCount == client.config.BatchSize) {
			if err := clientProtocol.SendBetBatch(betBatch); err != nil {
				logger.Error(mainAction, logger.Fail, "message", betBatch)
				return err
			}
			clear(betBatch)
			betBatch = betBatch[:0]
			betsCount = 0
			logger.Info("send-batch-complete", logger.Success, "batch-size", len(betBatch))
		}

		// Preparo el contenido de la row todo en bytes porque el buffer es []byte
		if len(betBatch) > 0 {
			betBatch = append(betBatch, '\n')
		}
		betBatch = append(betBatch, agencyIdInBytes...)
		betBatch = append(betBatch, ',')
		betBatch = append(betBatch, line...)
		betsCount++
	}

	if err := scanner.Err(); err != nil {
		logger.Error("file-read", logger.Fail, messageLog...)
		return err
	}

	// Si termino de leer el file, envío si tengo alguna bet en el batch
	if len(betBatch) > 0 {
		if err := clientProtocol.SendBetBatch(betBatch); err != nil {
			logger.Error(mainAction, logger.Fail, "message", betBatch)
			return err
		}
	}

	// Envío mensaje de que no envío más bets
	if err := clientProtocol.SendMessageBetsEnd(); err != nil {
		logger.Error("send-message-bets-end", logger.Fail, messageLog...)
		return err
	}
	logger.Info("send-message-bets-end", logger.Success, messageLog...)
	return nil
}


// Función encargada de recibir los ganadores y escribirlos en el archivo de salida.
// Recibe el protocolo del cliente.
func (client *Client) receiveWinners(clientProtocol *protocol.Protocol) error {
	messageLog := []any{"agency-id", client.config.AgencyId}

	// Creo archivo output-x.csv y lo abro como write only, si no existe lo crea.
	outputFile, err := os.OpenFile(client.config.OutputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, FILE_WRITE_PERMS)
	if err != nil {
		logger.Error("output-file-open", logger.Fail, messageLog...)
		return err
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	// Hago flush del buffer a disco cuando se llene el buffer (principalmente para la ultima iteración)
	defer writer.Flush() 
	
	// Recibo los ganadores y los persisto
	for {
		logger.Info("receive-winners", logger.InProgress, messageLog...)
		winner, err := clientProtocol.RecvWinner()
		if winner == "" {
			break
		}
		if err != nil {
			logger.Error("receive-winners", logger.Fail, messageLog...)
			break
		}

		if _, err := writer.WriteString(winner + "\n"); err != nil {
			logger.Error("output-write", logger.Fail, "err", err)
			return err
		}
		logger.Info("receive-winners", logger.Success, "agency-id", client.config.AgencyId)
	}
	logger.Info("output-written", logger.Success, messageLog...)
	return nil
}

// Función principal del cliente.
func (client *Client) Run() error {
	defer client.conn.Close()
	
	// Se estima un tamaño promedio (observación pesimista) de 128 bytes por bet
	// Se define que el buffer size para el bet sera 128 * el batch size para que
	// no ocurran reallocs.
	// De todas maneras si la bet que se está por leer no entra en el batch,
	// se envía lo que ya se tiene
	// (ver flujo de sendBets más arriba)
	const estimatedBetSize = 128
	betBufferSize := estimatedBetSize * client.config.BatchSize
	clientProtocol := protocol.NewProtocol(client.conn, betBufferSize)

	if err := client.sendBets(clientProtocol); err != nil {
		return err
	}

	if err := client.receiveWinners(clientProtocol); err != nil {
		return err
	}

	return nil
}

// Función invocada por handle_sigterm en caso de recibir SIGTERM. 
// Sólo cierra el socket del cliente.
func (client *Client) Stop() {
	if client.conn != nil {
		client.conn.Close()
	}
}