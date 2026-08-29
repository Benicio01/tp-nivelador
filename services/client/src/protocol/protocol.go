package protocol

import (
	"io"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const HEADER_SIZE = 2
const BATCH_SEPARATOR = "\n"
const BATCH_DEFAULT_SIZE = 16

var ACK_MARKER = []byte("OK")

func putUint16BigEndian(b []byte, v uint16) {
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}

func uint16BigEndian(b []byte) uint16 {
	return uint16(b[0])<<8 | uint16(b[1])
}

func EncodeFrame(payload []byte) []byte {
	frame := make([]byte, HEADER_SIZE, HEADER_SIZE+len(payload))
	putUint16BigEndian(frame, uint16(len(payload)))
	return append(frame, payload...)
}

func ReadFrame(r io.Reader) ([]byte, error) {
	header, err := safe_socket.RecvAll(r, HEADER_SIZE)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	size := int(uint16BigEndian(header))
	payload, err := safe_socket.RecvAll(r, size)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func MarshalBet(agency string, fields []string) []byte {
	record := append([]string{agency}, fields...)
	return []byte(strings.Join(record, ","))
}

func MarshalBatch(agency string, records [][]string) []byte {
	lines := make([]string, 0, len(records))
	for _, record := range records {
		lines = append(lines, string(MarshalBet(agency, record)))
	}
	return []byte(strings.Join(lines, BATCH_SEPARATOR))
}

func UnmarshalBet(payload []byte) []string {
	return strings.Split(string(payload), ",")
}

func UnmarshalBatch(payload []byte) [][]string {
	lines := strings.Split(string(payload), BATCH_SEPARATOR)
	records := make([][]string, 0, len(lines))
	for _, line := range lines {
		records = append(records, UnmarshalBet([]byte(line)))
	}
	return records
}
