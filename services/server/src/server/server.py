import socket
import sys
import logger
from lottery_monitor import LotteryMonitor
from .client_handler import ClientHandler


class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery_monitor = LotteryMonitor("bets.csv", agency_quorum_min)
        self.server_socket = None
        self.should_keep_running = True
        self.client_handlers = []

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            self.server_socket = server_socket
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while self.should_keep_running:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                    client_handler = ClientHandler(client_socket, self.lottery_monitor)
                    self.client_handlers.append(client_handler)
                    client_handler.start()
                    logger.info(action, logger.LogResult.success)
                except OSError as e:
                    if not self.should_keep_running:
                        break
                    raise e
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e

    def stop(self, signum, frame):
        logger.info("server-stop", logger.LogResult.in_progress)
        self.should_keep_running = False
        for client_handler in self.client_handlers:
            client_handler.stop()
        self.lottery_monitor.stop()
        if self.server_socket:
            self.server_socket.close()
        logger.info("server-stop", logger.LogResult.success)