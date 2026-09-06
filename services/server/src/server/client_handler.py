import threading
import logger
from protocol import Protocol, MessageType


class ClientHandler(threading.Thread):
    """
    ClientHandler se encarga de procesar los mensajes que envía el ciente de Go.
    Interpreta los mensajes, procesa los batches de bets, obtiene los ganadores y se los envía al cliente.
    """
    def __init__(self, client_socket, lottery_monitor):
        #Como hereda de threading.Thread, se debe llamar al init de Thread.
        super().__init__()
        self.client_socket = client_socket
        self.lottery_monitor = lottery_monitor
        self.should_keep_running = True
        self.agency_id = -1

    def run(self):
        action = "handle-client"
        message_amount = 0
        server_protocol = Protocol(self.client_socket)

        try:
            logger.info(action, logger.LogResult.in_progress)
            while self.should_keep_running:
                msg_type = server_protocol.recv_code() # Primero recibo el tipo de código (de tipo MessageType)

                if msg_type is None:
                    logger.error("recv_code", logger.LogResult.fail, "messages-amount", message_amount)
                    return

                # Recibe un batch de bets
                if msg_type == MessageType.BETS:
                    bets = server_protocol.recv_bets()
                    self.lottery_monitor.store_bets(bets) # Persisto los bets en el file (bloqueante)
                    server_protocol.send_batch_processed()
                    message_amount += len(bets)
                    logger.info("save-bets-disk", logger.LogResult.success, "bets-amount", len(bets))

                # Recibo primer mensaje handhsake con agency id
                elif msg_type == MessageType.HANDSHAKE:
                    self.agency_id = server_protocol.recv_agency_id()
                    logger.info(action, logger.LogResult.success, "handshake", "agency-id", self.agency_id)

                # Ya no voy a recibir mas batches de bets por lo que hago break.
                elif msg_type == MessageType.END_BETS:
                    logger.info(
                        action,
                        logger.LogResult.success,
                        "messages-amount",
                        message_amount,
                    )
                    break

            # Obtengo los ganadores y los envío al cliente
            logger.info("send-winners", logger.LogResult.in_progress, "agency-id", self.agency_id)
            winners = self.lottery_monitor.get_winners(self.agency_id)
            server_protocol.send_winning_bets(winners)
            logger.info("send-winners-done", logger.LogResult.success, "agency-id", self.agency_id)
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e
        finally:
            self.stop()

    def stop(self):
        """
        Detiene el client_handler. should_keep_running se vuelve False, lo que corta el while.
        Cierra el socket del cliente.
        """
        self.should_keep_running = False
        self.client_socket.close()
