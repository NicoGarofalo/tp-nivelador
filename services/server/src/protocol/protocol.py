import safe_socket
from bet import Bet

class Protocol:
    _BIG_ENDIAN_FORMAT = "big"
    _TWO_BYTES_SIZE = 2

    def __init__(self, socket):
        self.socket = socket

    def _serialize_number(self, number):
        return number.to_bytes(self._TWO_BYTES_SIZE, byteorder=self._BIG_ENDIAN_FORMAT)

    def _deserialize_number(self, number_buffer):
        return int.from_bytes(number_buffer, byteorder=self._BIG_ENDIAN_FORMAT)

    def _serialize_bet(self, bet):
        return f"{bet.agency_id},{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"

    def _deserialize_bet(self,serialized_bet):
        deserialized_bet = serialized_bet.split(',')
        return Bet(
            agency_id=int(deserialized_bet[0]),
            first_name=deserialized_bet[1],
            last_name=deserialized_bet[2],
            document=int(deserialized_bet[3]),
            birthdate=deserialized_bet[4],
            number=int(deserialized_bet[5])
        )
    
    def _send_message(self, data):
        message_length_serialized = self._serialize_number(len(data))
        safe_socket.send_all(self.socket, message_length_serialized)
        safe_socket.send_all(self.socket, data)

    def _recv_message(self):
        message_length_serialized = safe_socket.recv_all(self.socket, self._TWO_BYTES_SIZE)
        if not message_length_serialized:
            return None
        message_length = self._deserialize_number(message_length_serialized)
        message_serialized = safe_socket.recv_all(self.socket, message_length)
        if not message_serialized:
            return None
        message_deserialized = message_serialized.split(',')

        return message_deserialized

    def recv_bet(self):
        message_length_serialized = safe_socket.recv_all(self.socket, self._TWO_BYTES_SIZE)
        if not message_length_serialized:
            return None
        message_length = self._deserialize_number(message_length_serialized)
        message_serialized = safe_socket.recv_all(self.socket, message_length)
        if not message_serialized:
            return None
        
        return self._deserialize_bet(message_serialized)
    
    def recv_agency_id(self):
        message_deserialized = self._recv_message()
        return int(message_deserialized[0])
    
    def send_winning_bets(self, winning_bets):
        for bet in winning_bets:
            self._send_message(self._serialize_bet(bet))