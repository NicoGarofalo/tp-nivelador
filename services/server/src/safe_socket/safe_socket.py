import socket

def recv_all(socket: socket.socket, size):
    total_bytes_received = 0
    buffer = bytearray()
    while total_bytes_received < size:
        bytes_to_receive = size - len(buffer)
        msg_received = socket.recv(bytes_to_receive)
        if not msg_received:
            return None
        buffer.extend(msg_received)
        total_bytes_received += len(msg_received)
    return bytes(buffer)


def send_all(socket: socket.socket, bytes):
    total_bytes_sent = 0
    bytes_to_send = len(bytes)
    while total_bytes_sent < bytes_to_send:
        bytes_sent = socket.send(bytes[total_bytes_sent:])
        total_bytes_sent += bytes_sent
    return total_bytes_sent
