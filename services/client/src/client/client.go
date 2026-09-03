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

func (client *Client) Run() error {
	const mainAction = "test-send-bet"
	defer client.conn.Close()
	
	const estimatedBetSize = 128
	betBufferSize := estimatedBetSize * client.config.BatchSize

	messageLog := []any{"agency-id", client.config.AgencyId}

	// Abro archivo input-x.csv
	file, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("input-file-open", logger.Fail, messageLog...)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	clientProtocol := protocol.NewProtocol(client.conn)

	// Envío agency id para comenzar comunicacion
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
		
		if len(betBatch) > 0 && (len(betBatch) + sizeNeeded > betBufferSize || betsCount == client.config.BatchSize) {
			if err := clientProtocol.SendBetBatch(betBatch); err != nil {
				logger.Error(mainAction, logger.Fail, "message", betBatch)
				return err
			}
			clear(betBatch)
			betBatch = betBatch[:0]
			betsCount = 0
		}

		if len(betBatch) > 0 {
			betBatch = append(betBatch, '\n')
		}

		// Preparo el contenido de la row
		betBatch = append(betBatch, agencyIdInBytes...)
		betBatch = append(betBatch, ',')
		betBatch = append(betBatch, line...)
		betsCount++
	}

	if err := scanner.Err(); err != nil {
		logger.Error("file-read", logger.Fail, messageLog...)
		return err
	}

	if len(betBatch) > 0 {
		if err := clientProtocol.SendBetBatch(betBatch); err != nil {
			logger.Error(mainAction, logger.Fail, "message", betBatch)
			return err
		}
	}

	if err := clientProtocol.SendMessageBetsEnd(); err != nil {
		logger.Error("send-message-bets-end", logger.Fail, messageLog...)
		return err
	}
	logger.Info("send-message-bets-end", logger.Success, messageLog...)
	
	// Recibo los ganadores
	winners := []string{}
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
		winners = append(winners, winner)
	}

	// Creo archivo output-x.csv y lo abro
	outputFile, err := os.OpenFile(client.config.OutputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Error("output-file-open", logger.Fail, messageLog...)
		return err
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	for _, winner := range winners {
		if _, err := writer.WriteString(winner + "\n"); err != nil {
			logger.Error("output-write", logger.Fail, "err", err)
			return err
		}
		logger.Info("output-written", logger.Success, "agency-id", client.config.AgencyId, "content", winner)
	}
	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)

	return nil
}

func (client *Client) Stop() {
	if client.conn != nil {
		client.conn.Close()
	}
}