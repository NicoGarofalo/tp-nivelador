import safe_socket
from lottery import Bet
from enum import Enum, auto

# Codigos que entiende el client_handler.
class MessageType(Enum):
    HANDSHAKE = auto()
    BETS = auto()
    END_BETS = auto()


class Protocol:
    _BIG_ENDIAN_FORMAT = "big"
    _LENGTH_BYTES_SIZE = 2
    _CODE_BYTE_SIZE = 1

    #Codigos interno entre protocolos
    _INTERNAL_HANDHSAKE_CODE = 0x01
    _INTERNAL_BATCH_BET_CODE = 0x02
    _INTERNAL_BATCH_ACK = 0x03
    _INTERNAL_WINNER_CODE = 0x04
    _INTERNAL_END_BETS_CODE = 0x05
    _INTERNAL_END_WINNERS_CODE = 0x06

    # Diccionario interno para mapear INTERNAL_CODES a MessageType.
    _CODE_TO_MESSAGE_TYPE = {
        _INTERNAL_HANDHSAKE_CODE: MessageType.HANDSHAKE,
        _INTERNAL_BATCH_BET_CODE: MessageType.BETS,
        _INTERNAL_END_BETS_CODE: MessageType.END_BETS
    }



    def __init__(self, socket):
        self.socket = socket
    
    def _get_message_type(self, code):
        """
        Dado un INTERNAL_CODE, devuelve el MessageType asociado.
        """
        if not code or code[0] not in self._CODE_TO_MESSAGE_TYPE:
            return None
        return self._CODE_TO_MESSAGE_TYPE[code[0]]

    def _serialize_number(self, number):
        """
        Serializa un numero a formato big-endian y de 2 bytes.
        """
        return number.to_bytes(self._LENGTH_BYTES_SIZE, byteorder=self._BIG_ENDIAN_FORMAT)

    def _deserialize_number(self, number_buffer):
        """
        Deserializa un numero que ocupa 2 bytes y llega en formato big-endian.
        Retorna el numero deserializado en formato int.
        """
        return int.from_bytes(number_buffer, byteorder=self._BIG_ENDIAN_FORMAT)

    def _serialize_bet(self, bet):
        """
        Serializa, a partir de un struct bet, a un string de bet encodeado.
        Retorna la bet serializada.
        """
        bet_stringified = f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"
        return bet_stringified.encode('utf-8')

    def _deserialize_bet(self, serialized_bet):
        """
        Deserializa una bet de un mensaje.
        Se encarga de mapear cada campo deserializado a su tipo correspondiente para armar el struct bet.
        Retorna una instancia de struct Bet.
        """
        deserialized_bet = serialized_bet.split(',')
        return Bet(
            agency_id=int(deserialized_bet[0]),
            first_name=deserialized_bet[1],
            last_name=deserialized_bet[2],
            document=int(deserialized_bet[3]),
            birthdate=deserialized_bet[4],
            number=int(deserialized_bet[5])
        )

    def _deserialize_batch_bets(self,serialized_batch_bet):
        """
        Deserializa un batch de bets de un mensaje.
        Se encarga principalmente de realizar el decode y el split de las apuestas.
        Retorna el batch de bets deserializado, es decir, como array de struct Bets.
        """
        batches_splitted = serialized_batch_bet.decode('utf-8').split('\n')
        bet_list = []
        for bet_string in batches_splitted:
            bet_list.append(self._deserialize_bet(bet_string))
        return bet_list

    def _deserialize_agency_id(self, agency_id_serialized):
        """
        Deserializa el agency_id de un mensaje.
        Retorna el agency_id de tipo int.
        """
        agency_id = agency_id_serialized.decode('utf-8')
        return int(agency_id)

    def _send_message(self, code, data=b""):
        """
        Envía un mensaje con el formato CODE | MSG_SIZE | MESSAGE.
        data puede ser vacío, en cuyo caso solo se envía el código.
        """
        message = bytearray()
        message.append(code)
        if data:
            message_length_serialized = self._serialize_number(len(data))
            message.extend(message_length_serialized)
            message.extend(data)
        safe_socket.send_all(self.socket, message)

    def _recv_message(self):
        """
        Recibe el contenido MSG_SIZE | MESSAGE de un mensaje completo con formato CODE | MSG_SIZE | MESSAGE. 
        Recibe primero MSG_SIZE en 2 bytes, se deserializa y luego se recibe el resto del mensaje.
        Devuelve el mensaje serializado.
        """
        message_length_serialized = safe_socket.recv_all(self.socket, self._LENGTH_BYTES_SIZE)
        if not message_length_serialized:
            return None
        message_length = self._deserialize_number(message_length_serialized)
        message_serialized = safe_socket.recv_all(self.socket, message_length)
        if not message_serialized:
            return None

        return message_serialized

    def recv_bets(self):
        """
        Recibe un batch de bets, junto al código Batch Bet vinculado.
        Retorna el batch de bets deserializado, es decir, como array de struct Bets.
        """
        message_serialized = self._recv_message()
        return self._deserialize_batch_bets(message_serialized)

    def send_batch_processed(self):
        """
        Envía un mensaje con Código ACK de finalización de procesamiento de un batch de bets.
        """
        self._send_message(self._INTERNAL_BATCH_ACK)
    
    def recv_agency_id(self):
        """
        Recibe el agency id que llega junto al mensaje con code Handshake.
        Retorna el agency_id deserializado.
        """
        message_serialized = self._recv_message()
        if message_serialized is None:
            return None
        return self._deserialize_agency_id(message_serialized)
    
    def send_winning_bets(self, winning_bets):
        """
        Envía los ganadores al cliente. 
        Cuando se enviaron todos los ganadores, se envía codigo de fin de envío _INTERNAL_END_WINNERS_CODE.
        """
        # Envio winners
        for bet in winning_bets:
            self._send_message(self._INTERNAL_WINNER_CODE, self._serialize_bet(bet))
        # Envio codigo de fin
        self._send_message(self._INTERNAL_END_WINNERS_CODE)

    def recv_code(self):
        """
        Recibe la sección CODE (que ocupa 1 byte), del mensaje con formato CODE | MSG_SIZE | MESSAGE. 
        Devuelve el tipo de mensaje asociado, comprendido por el Client handler.
        """
        internal_code = safe_socket.recv_all(self.socket, self._CODE_BYTE_SIZE)
        if internal_code is None:
            return None
        return self._get_message_type(internal_code)