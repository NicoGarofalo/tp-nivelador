package main

import (
	"errors"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	client "github.com/7574-sistemas-distribuidos/tp-nivelador/src/client"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

const EXIT_OK_CODE = 0
const EXIT_ERROR_CODE = 1

func loadConfig() (client.ClientConfig, error) {
	agencyId := os.Getenv("AGENCY_ID")
	if agencyId == "" {
		return client.ClientConfig{}, errors.New("AGENCY_ID environment variable is required")
	}

	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	inputFile := os.Getenv("INPUT_FILE")
	if inputFile == "" {
		return client.ClientConfig{}, errors.New("INPUT_FILE environment variable is required")
	}

	outputFile := os.Getenv("OUTPUT_FILE")
	if outputFile == "" {
		return client.ClientConfig{}, errors.New("OUTPUT_FILE environment variable is required")
	}

	batchSizeStr := os.Getenv("BATCH_SIZE")
	if batchSizeStr == "" {
		return client.ClientConfig{}, errors.New("BATCH_SIZE environment variable is required")
	}
	batchSize, err := strconv.Atoi(batchSizeStr)
	if err != nil {
		return client.ClientConfig{}, errors.New("BATCH_SIZE environment variable must be an integer")
	}

	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		AgencyId:   agencyId,
		InputFile: inputFile,
		OutputFile: outputFile,
		BatchSize: batchSize,
	}, nil
}

// Se define una goroutine que ejecuta handle_sigterm. 
// En caso de que el SO reciba SIGTERM, se recibirá la notificacion en el canal sigChan, desbloqueando el flujo.
// Mientras eso no ocurra, la rutina estará bloqueada (ya que no llegan mensajes al canal)
func handle_sigterm(sigChan chan os.Signal, c *client.Client, agencyId string) {
	signal := <- sigChan
	logger.Info("handle-sigterm", logger.Success, "signal received", signal, "agency-id", agencyId)
	c.Stop()
	os.Exit(EXIT_OK_CODE)
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		logger.Error("load-config", logger.Fail, "err", err)
		return EXIT_ERROR_CODE
	}
	
	client, err := client.NewClient(config)
	if err != nil {
		logger.Error("client-new", logger.Fail, "err", err)
		return EXIT_ERROR_CODE
	}

	const chan_buff_size = 1
	sigChan := make(chan os.Signal, chan_buff_size)
	// Defino que si llega SIGTERM, se le notifique a sigChan.
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go handle_sigterm(sigChan, client, config.AgencyId)

	if err := client.Run(); err != nil {
		logger.Error("client-run", logger.Fail, "err", err)
		return EXIT_ERROR_CODE
	}
	return EXIT_OK_CODE
}

func main() {
	os.Exit(run())
}
