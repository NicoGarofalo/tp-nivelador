import socket
import logger
import protocol
from lottery import Lottery



class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery("../../output/bets.csv")

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        server_protocol = protocol.Protocol(client_socket)
        # Recibo agency id como primer mensaje o handshake.
        agency_id = server_protocol.recv_agency_id()
        logger.info(action, logger.LogResult.in_progress, "handhsake-agency-id", agency_id)
        client_bets = []
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                client_bet = server_protocol.recv_bet()
                if not client_bet:
                    logger.info(
                        action,
                        logger.LogResult.success,
                        "messages-amount",
                        message_amount,
                    )
                    return
                message_amount += 1
                client_bets.append(client_bet)
            self.lottery.store_bets(client_bets)
            all_bets = self.lottery.load_bets()
            winning_bets = []
            for bet in all_bets:
                if agency_id == bet.agency_id and self.lottery.has_won(bet):
                    winning_bets.append(bet)
            
            server_protocol.send_winning_bets(winning_bets)
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e

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

                self._handle_client(client_socket)
