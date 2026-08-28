import socket

import safe_socket

HEADER_SIZE = 2
BATCH_SEPARATOR = "\n"

ACK = b"OK"

def encode_frame(payload: bytes) -> bytes:
    length = len(payload).to_bytes(HEADER_SIZE, byteorder="big")
    return length + payload


def read_frame(sock: socket.socket):
    header = safe_socket.recv_all(sock, HEADER_SIZE)
    if not header:
        return None
    if len(header) < HEADER_SIZE:
        raise ConnectionError("truncated message header")
    payload_size = int.from_bytes(header, "big")
    payload = safe_socket.recv_all(sock, payload_size)
    if len(payload) < payload_size:
        raise ConnectionError("truncated message payload")
    return payload

def marshal_bet(agency_id, fields) -> bytes:
    return ",".join([str(agency_id), *fields]).encode()


def unmarshal_bet(payload: bytes):
    return payload.decode().split(",")


def unmarshal_batch(payload: bytes):
    lines = payload.decode().split(BATCH_SEPARATOR)
    return [unmarshal_bet(line.encode()) for line in lines]