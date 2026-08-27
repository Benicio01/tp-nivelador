import socket
import logger
import safe_socket

_MESSAGE_HEADER_SIZE = 2


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                header = safe_socket.recv_all(
                    client_socket, _MESSAGE_HEADER_SIZE
                )
                if not header:
                    logger.info(
                        action,
                        logger.LogResult.success,
                        "messages-amount",
                        message_amount,
                    )
                    return
                if len(header) < _MESSAGE_HEADER_SIZE:
                    raise ConnectionError("truncated message header")
                payload_size = int.from_bytes(header, "big")
                payload = safe_socket.recv_all(
                    client_socket, payload_size
                )
                if len(payload) < payload_size:
                    raise ConnectionError("truncated message payload")
                message_amount += 1
                safe_socket.send_all(client_socket, header + payload)
        except Exception as e:
            logger.error(
                action,
                logger.LogResult.fail,
                "messages-amount",
                message_amount,
                "err",
                e,
            )

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                try:
                    self._handle_client(client_socket)
                finally:
                    client_socket.close()
