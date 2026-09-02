import threading
import logger
from protocol import Protocol, MessageType


class ClientHandler(threading.Thread):
    def __init__(self, client_socket, lottery_monitor):
        super().__init__()
        self.client_socket = client_socket
        self.lottery_monitor = lottery_monitor
        self.should_keep_running = True

    def run(self):
        action = "handle-client"
        message_amount = 0
        server_protocol = Protocol(self.client_socket)
        agency_id = -1

        try:
            logger.info(action, logger.LogResult.in_progress)
            while self.should_keep_running:
                msg_type = server_protocol.recv_code()

                if msg_type is None:
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

            winning_bets = self.lottery_monitor.get_winners(agency_id)

            logger.info("send-winners", logger.LogResult.in_progress, "agency-id", agency_id, "bets-amount", len(winning_bets))
            server_protocol.send_winning_bets(winning_bets)
            logger.info("send-winners", logger.LogResult.success, "agency-id", agency_id, "bets-amount", len(winning_bets))
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e
        finally:
            self.stop()

    def stop(self):
        self.should_keep_running = False
        self.client_socket.close()
