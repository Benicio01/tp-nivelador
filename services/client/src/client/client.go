package client

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
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

func (client *Client) Close() error {
	return client.conn.Close()
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
	const mainAction = "process-bets"
	defer client.conn.Close()

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "err", err)
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("open-output-file", logger.Fail, "err", err)
		return err
	}
	defer outputFile.Close()

	reader := csv.NewReader(inputFile)
	batchSize := client.config.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	batch := make([][]string, 0, batchSize)
	messageId := 0
	for {
		record, err := reader.Read()

		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Error("read-input-file", logger.Fail, "err", err)
			return err
		}
		if len(record) == 0 {
			continue
		}

		batch = append(batch, record)
		if len(batch) < batchSize {
			continue
		}

		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId, "batch-size", len(batch)}
		logger.Info(mainAction, logger.InProgress, messageArgs...)

		payload := protocol.MarshalBatch(client.config.AgencyId, batch)
		frame := protocol.EncodeFrame(payload)
		if err := safe_socket.SendAll(client.conn, frame); err != nil {
			if isClosedConnError(err) {
				return nil
			}
			logger.Error("send-bet", logger.Fail, messageArgs...)
			return err
		}

		if err := client.awaitAck(); err != nil {
			if isClosedConnError(err) {
				return nil
			}
			logger.Error("await-ack", logger.Fail, messageArgs...)
			return err
		}

		batch = batch[:0]
		messageId++
	}

	if len(batch) > 0 {
		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId, "batch-size", len(batch)}
		logger.Info(mainAction, logger.InProgress, messageArgs...)

		payload := protocol.MarshalBatch(client.config.AgencyId, batch)
		frame := protocol.EncodeFrame(payload)
		if err := safe_socket.SendAll(client.conn, frame); err != nil {
			if isClosedConnError(err) {
				return nil
			}
			logger.Error("send-bet", logger.Fail, messageArgs...)
			return err
		}

		if err := client.awaitAck(); err != nil {
			if isClosedConnError(err) {
				return nil
			}
			logger.Error("await-ack", logger.Fail, messageArgs...)
			return err
		}
	}

	if err := client.closeWrite(); err != nil {
		return err
	}

	for {
		payload, err := protocol.ReadFrame(client.conn)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if isClosedConnError(err) {
				return nil
			}
			logger.Error("recv-winner", logger.Fail, "err", err)
			return err
		}

		fields := protocol.UnmarshalBet(payload)
		if _, err := outputFile.WriteString(strings.Join(fields[1:], ",") + "\n"); err != nil {
			logger.Error("persist-winner", logger.Fail, "err", err)
			return err
		}
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)
	return nil
}

func (client *Client) awaitAck() error {
	payload, err := protocol.ReadFrame(client.conn)
	if err != nil {
		return err
	}
	if string(payload) != string(protocol.ACK_MARKER) {
		return fmt.Errorf("unexpected ack payload: %q", payload)
	}
	return nil
}

func (client *Client) closeWrite() error {
	tcpConn, ok := client.conn.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("connection is not *net.TCPConn")
	}
	if err := tcpConn.CloseWrite(); err != nil {
		if isClosedConnError(err) {
			return nil
		}
		logger.Error("close-write", logger.Fail, "err", err)
		return err
	}
	return nil
}

func isClosedConnError(err error) bool {
	return errors.Is(err, net.ErrClosed)
}
