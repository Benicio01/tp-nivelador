import signal
import socket
import threading

import logger
import protocol
import safe_socket
from lottery import Lottery, Bet

TIMEOUT_SECONDS = 1.0

class Server:
    def __init__(
        self, server_host: str, server_port: int, lottery: Lottery, agency_quorum_min: int
    ) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = lottery
        self.agency_quorum_min = agency_quorum_min
        self._lottery_lock = threading.Lock()
        self._quorum_cond = threading.Condition()
        self._arrived_agencies = 0
        self._quorum_reached = False
        self._shutdown = threading.Event()
        self._client_sockets_lock = threading.Lock()
        self._client_sockets = set()

    def _store_bets(self, bets):
        with self._lottery_lock:
            self.lottery.store_bets(bets)

    def _handle_client(self, client_socket):
        action = "handle-client"
        bets_count = 0
        agency_id = None
        try:
            while True:
                payload = protocol.read_frame(client_socket)
                if payload is None:
                    break
                batch_columns = protocol.unmarshal_batch(payload)
                batch_bets = []
                for columns in batch_columns:
                    bet = Bet(
                        int(columns[0]),
                        columns[1],
                        columns[2],
                        int(columns[3]),
                        columns[4],
                        int(columns[5]),
                    )
                    batch_bets.append(bet)
                if not batch_bets:
                    continue
                self._store_bets(batch_bets)
                if agency_id is None:
                    agency_id = batch_bets[0].agency_id
                self._ack_batch(client_socket)
                bets_count += len(batch_bets)

            if agency_id is None:
                return

            with self._quorum_cond:
                self._arrived_agencies += 1
                if self._arrived_agencies >= self.agency_quorum_min:
                    self._quorum_reached = True
                    self._quorum_cond.notify_all()
                
                while not self._quorum_reached and not self._shutdown.is_set():
                    self._quorum_cond.wait()

            if self._shutdown.is_set():
                return

            with self._lottery_lock:
                for bet in self.lottery.load_bets():
                    if bet.agency_id == agency_id and self.lottery.has_won(bet):
                        self._send_winner(client_socket, bet)

            logger.info(
                action,
                logger.LogResult.success,
                "agency-id",
                agency_id,
                "messages-amount",
                bets_count,
            )
        except Exception as e:
            if not self._shutdown.is_set():
                logger.error(
                    action,
                    logger.LogResult.fail,
                    "err",
                    e,
                )
        finally:
            with self._client_sockets_lock:
                self._client_sockets.discard(client_socket)
            client_socket.close()

    def _ack_batch(self, client_socket):
        frame = protocol.encode_frame(protocol.ACK)
        safe_socket.send_all(client_socket, frame)

    def _send_winner(self, client_socket, bet):
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
        signal.signal(signal.SIGTERM, self._signal_handler)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            server_socket.settimeout(TIMEOUT_SECONDS)
            
            logger.info(action, logger.LogResult.in_progress)
            while not self._shutdown.is_set():
                try:
                    client_socket, _ = server_socket.accept()
                except socket.timeout:
                    continue
                logger.info(action, logger.LogResult.success)

                with self._client_sockets_lock:
                    self._client_sockets.add(client_socket)

                worker = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,),
                    daemon=True,
                )
                worker.start()
            self._close_client_sockets()

    def _close_client_sockets(self):
        with self._client_sockets_lock:
            sockets = list(self._client_sockets)
        for sock in sockets:
            try:
                sock.close()
            except OSError:
                pass
        with self._quorum_cond:
            self._quorum_cond.notify_all()

    def _signal_handler(self, signum, frame):
        self._shutdown.set()
