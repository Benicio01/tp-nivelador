package safe_socket

import "io"

func SendAll(socket io.Writer, bytes []byte) error {
	written := 0
	for written < len(bytes) {
		n, err := socket.Write(bytes[written:])
		written += n
		if err != nil {
			return err
		}
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	received := 0
	for received < size {
		n, err := socket.Read(buff[received:])
		received += n
		if err == io.EOF {
			if received < size {
				return nil, io.ErrUnexpectedEOF
			}
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return buff[:received], nil
}
