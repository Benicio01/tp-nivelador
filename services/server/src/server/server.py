import socket
import logger
import protocol
import safe_socket
from lottery import Lottery, Bet


class Server:
    def __init__(self, server_host: str, server_port: int, lottery: Lottery) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = lottery

    def _handle_client(self, client_socket):
        action = "handle-client"
        bets = []
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                payload = protocol.read_frame(client_socket)
                if payload is None:
                    break
                columns = protocol.unmarshal_bet(payload)
                bet = Bet(
                    int(columns[0]),
                    columns[1],
                    columns[2],
                    int(columns[3]),
                    columns[4],
                    int(columns[5]),
                )
                bets.append(bet)
            if not bets:
                return

            agency_id = bets[0].agency_id
            self.lottery.store_bets(bets)
            self._send_winners(client_socket, agency_id)
            logger.info(
                action,
                logger.LogResult.success,
                "messages-amount",
                len(bets),
                "agency-id",
                agency_id,
            )
        except Exception as e:
            logger.error(
                action,
                logger.LogResult.fail,
                "messages-amount",
                len(bets),
                "err",
                e,
            )

    def _send_winners(self, client_socket, agency_id):
        for bet in self.lottery.load_bets():
            if bet.agency_id == agency_id and self.lottery.has_won(bet):
                payload = protocol.marshal_bet(
                    bet.agency_id,
                    [
                        bet.first_name,
                        bet.last_name,
                        str(bet.document),
                        bet.birthdate,
                        str(bet.number),
                    ],
                )
                frame = protocol.encode_frame(payload)
                safe_socket.send_all(client_socket, frame)

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
