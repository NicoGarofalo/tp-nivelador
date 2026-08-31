import socket
import logger
from protocol import Protocol, MessageType
from lottery import Lottery



class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery("bets.csv")

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0

        server_protocol = Protocol(client_socket)

        # Recibo agency id como primer mensaje o handshake.
        agency_id = -1
        
        client_bets = []
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                msg_type = server_protocol.recv_code()

                if msg_type == None:
                    logger.error("recv_code", logger.LogResult.fail, "messages-amount", message_amount)
                    return
                
                if msg_type == MessageType.BETS:
                    bets = server_protocol.recv_bets()
                    self.lottery.store_bets(bets)
                    server_protocol.send_batch_processed()
                    message_amount += len(bets)
                    logger.info("save-bets-disk", logger.LogResult.success, "bets-amount", len(bets))
                
                elif msg_type == MessageType.HANDSHAKE:
                    agency_id = server_protocol.recv_agency_id()
                    logger.info(action, logger.LogResult.success, "handshake", "agency-id", agency_id)
                    
                elif msg_type == MessageType.END_BETS:
                    logger.info(
                        action,
                        logger.LogResult.success,
                        "messages-amount",
                        message_amount,
                    )
                    break

            # Cuento los ganadores
            all_bets = self.lottery.load_bets()
            winning_bets = []
            for bet in all_bets:
                if agency_id == bet.agency_id and self.lottery.has_won(bet):
                    winning_bets.append(bet)

            # Envío los ganadores al client x
            logger.info("send-winners", logger.LogResult.in_progress, "agency-id", agency_id, "bets-amount", len(winning_bets))
            server_protocol.send_winning_bets(winning_bets)
            logger.info("send-winners", logger.LogResult.success, "agency-id", agency_id, "bets-amount", len(winning_bets))
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
