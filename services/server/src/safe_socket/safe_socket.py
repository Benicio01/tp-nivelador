import socket

def recv_all(socket: socket.socket, size, allow_eof=False):
    data = bytearray()
    while len(data) < size:
        chunk = socket.recv(size - len(data))
        if not chunk:
            if allow_eof:
                return bytes(data)
            continue
        data += chunk
    return bytes(data)


def send_all(socket: socket.socket, data):
    view = memoryview(data)
    while len(view):
        sent = socket.send(view)
        view = view[sent:]