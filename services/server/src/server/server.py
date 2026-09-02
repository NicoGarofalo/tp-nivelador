import socket
import logger
import threading
from protocol import Protocol, MessageType
from lottery_monitor import LotteryMonitor



class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery_monitor = LotteryMonitor("bets.csv", agency_quorum_min)

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
                    self.lottery_monitor.store_bets(bets)
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
            winning_bets = self.lottery_monitor.get_winners(agency_id)

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
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket,)
                    )
                    client_thread.start()
                    logger.info(action, logger.LogResult.success)
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
