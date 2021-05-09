import socket
import struct
import zlib
import threading
import queue
import time
import constants
from os.path import basename

STOP = False
KEEP_ALIVE_FLAG = threading.Event()


# main client function
def client_login(client_sock, server_addr):
    data, file_path, frag_size, corrupted, lost = initialize_message_data(client_sock, server_addr)
    global KEEP_ALIVE_FLAG
    # keep alive is no longer needed
    KEEP_ALIVE_FLAG.set()
    # if file name was not filled, then a message is being sent
    if file_path == "":
        data_type = constants.TEXT_TYPE
    else:
        data_type = constants.FILE_TYPE
    fragments = fragmentize(data, frag_size)
    init_header = struct.pack("iIc", len(fragments), data_type, str.encode("I", constants.CODING))
    if not initialize_communication(client_sock, server_addr, init_header):
        return
    # send file name
    if data_type == constants.FILE_TYPE:
        file_name = basename(file_path)
        client_sock.sendto(struct.pack("iIc", 0, 0, str.encode("N", constants.CODING))
                           + str.encode(file_name, constants.CODING), server_addr)
    # queue of fragments and sequence numbers
    to_send = queue.Queue()
    seq = 0
    # add fragments to queue
    for fragment in fragments:
        if seq not in lost:
            to_send.put((fragment, seq))
        seq += 1
    global STOP
    STOP = False
    # new thread to send data
    send_data = threading.Thread(target=send_fragments,
                                 args=(to_send, data_type, corrupted, client_sock, server_addr))
    print("Posielam data")
    send_data.start()
    # main thread keeps listening for server responses
    client_sock.settimeout(60)
    while not STOP:
        try:
            response = client_sock.recv(constants.BUFFER_SIZE)
        except socket.timeout:
            print("Data neboli uspesne dorucene do casoveho limitu (60sec)")
            break
        seq, _, code = struct.unpack("iIc", response[:constants.HEADER_SIZE])
        # if fragment was corrupted add it to send queue again
        if code.decode(constants.CODING) == "R":
            to_send.put((fragments[seq], seq))
        # if fragments are missing add them to queue
        if code.decode(constants.CODING) == "M":
            missing = response[constants.HEADER_SIZE:].decode(constants.CODING).split(";")
            for seq_num in missing:
                seq_num = int(seq_num)
                to_send.put((fragments[seq_num], seq_num))
        # if communication is finished, the sending thread is stopped
        if code.decode(constants.CODING) == "F":
            print("Data uspesne dorucene")
            STOP = True
            send_data.join()
            break

    client_logout(client_sock, server_addr)


# turns data into fragments
def fragmentize(data, frag_size):
    fragments = []
    i = 0
    while (i * frag_size) < len(data):
        fragments.append(data[i * frag_size: (i + 1) * frag_size])
        i += 1
    return fragments


# creates header for a fragment
def create_header(fragment, seq_num, packet_type, corrupt):
    fcs = zlib.crc32(fragment)
    if corrupt:
        fcs -= 1
    header = struct.pack("iIc", seq_num, fcs, str.encode(str(packet_type), constants.CODING))
    return header


# sends keep alive packets (used with a new thread)
def send_keep_alive(client_sock, server_addr):
    keep_alive_packet = struct.pack("iIc", 0, 0, str.encode("K"))
    # global var to stop the thread
    client_sock.settimeout(3)
    while True:
        client_sock.sendto(keep_alive_packet, server_addr)
        try:
            client_sock.recv(constants.BUFFER_SIZE)
        except socket.timeout:
            # if no response from server, print it and end the thread
            print("Server neodpovedal na Keep-Alive")
            return
        if KEEP_ALIVE_FLAG.wait(timeout=constants.KEEP_ALIVE_INTERVAL - 2):
            return


# sends fragments from queue (used with a new thread)
# thread keeps running until F packet from server is received on the main thread
def send_fragments(to_send, packet_type, corrupted, client_sock, server_addr):
    # global var to stop the thread
    global STOP
    i = 1
    while not STOP:
        if not to_send.empty():
            corrupt = False
            fragment, seq = to_send.get()
            # "throttling" (server can not handle data as fast as the client would send it)
            if i % 3 == 0:
                time.sleep(0.000000001)
            i += 1
            if seq in corrupted:
                corrupt = True
                corrupted.remove(seq)
            header = create_header(fragment, seq, packet_type, corrupt)
            client_sock.sendto(header + fragment, server_addr)


# sends init packet to server
def initialize_communication(client_sock, server_addr, init_header):
    client_sock.settimeout(5)
    try_again = "1"
    while try_again == "1":
        try:
            client_sock.sendto(init_header, server_addr)
            response = client_sock.recv(constants.BUFFER_SIZE)
            try_again = ""
        except socket.timeout:
            try_again = input("Ziadna odozva na inicializacny paket\nSkusit znova?"
                              "\n1 - ano\n2 - nie\n")
            if try_again == "2":
                return False
    print("Pripojeny na adresu ", server_addr, "\n")
    return True


# user input
def initialize_message_data(client_sock, server_addr):
    global KEEP_ALIVE_FLAG
    file_path = ""
    keep_alive = input("Zapnut Keep-Alive?:\n1 - ano\n2 - nie\n")
    if keep_alive == "1":
        KEEP_ALIVE_FLAG.clear()
        keep_alive_thread = threading.Thread(target=send_keep_alive, args=(client_sock, server_addr))
        keep_alive_thread.daemon = True
        keep_alive_thread.start()
    data_type = int(input("Poslat spravu alebo subor:\n1 - sprava\n2 - subor\n"))
    data = bytes()
    if data_type == 1:
        data = str.encode(input("Zadaj spravu:\n"), constants.CODING)
    if data_type == 2:
        file_path = input("Zadaj cestu k suboru\n")
        file = open(file_path, "rb")
        data = file.read()
        file.close()
    frag_size = 0
    while frag_size < 1 or frag_size > 1463:
        frag_size = int(input("Zadaj max. velkost fragmentu (1 az 1463):\n"))
    corruption = int(input("Simulovat chybu v fragmentoch?:\n1 - ano\n2 - nie\n"))
    corrupted = []
    if corruption == 1:
        corrupted = [int(item) for item in input("Zadaj poradove cisla chybnych fragmentov "
                                                 "(cisla oddelene medzerou):\n").split()]
    lose_packets = int(input("Simulovat stratu paketov?:\n1 - ano\n2 - nie\n"))
    lost = []
    if lose_packets == 1:
        lost = [int(item) for item in input("Zadaj poradove cisla stratenych fragmentov "
                                            "(cisla oddelene medzerou):\n").split()]

    return data, file_path, frag_size, corrupted, lost


# ----- server functions -----
def server_login(server_sock):
    print("Server pocuva na porte", server_sock.getsockname()[1])
    server_sock.settimeout(constants.KEEP_ALIVE_INTERVAL)
    packet_type = bytes()
    msg_type = ""
    packet_amount = ""
    addr = ""
    file_name = ""
    packet = bytes()
    # waiting for keep alive packets, if no packet is received, server is turned off
    # if init packet is received server responses and awaits data
    while packet_type.decode(constants.CODING) != "I":
        try:
            packet, addr = server_sock.recvfrom(constants.BUFFER_SIZE)
            print("Kept alive")
            packet_amount, msg_type, packet_type = struct.unpack("iIc", packet)
            if packet_type.decode(constants.CODING) == "K":
                server_sock.sendto(struct.pack("iIc", 0, 0, str.encode("K", constants.CODING)), addr)
        except socket.timeout:
            print("Server neprijal ziadne pakety")
            return
    server_sock.sendto(struct.pack("iIc", 0, 0, str.encode("A", constants.CODING)), addr)
    if msg_type == constants.FILE_TYPE:
        packet_type = bytes()
        # server waits for packet with file name
        while packet_type.decode(constants.CODING) != "N":
            packet = server_sock.recv(constants.BUFFER_SIZE)
            _, _, packet_type = struct.unpack("iIc", packet[:constants.HEADER_SIZE])
        file_name = packet[constants.HEADER_SIZE:].decode(constants.CODING)
    print("Ocakavany pocet paketov:", packet_amount)
    fragments = sorted(receive_data(server_sock, packet_amount, addr), key=get_seq_num)

    # if a message was received, it is saved into a bytes object then decoded and printed
    # if a file was received, it is saved into server folder
    if msg_type == constants.TEXT_TYPE:
        # message needs to be decoded at once due to characters that might be distributed among more fragments
        msg = bytes()
        print("sprava:", end="")
        for fragment in fragments:
            msg += fragment[constants.HEADER_SIZE:]
        print(msg.decode(constants.CODING))
    elif msg_type == constants.FILE_TYPE:
        with open("server/" + file_name, "wb") as file:
            for fragment in fragments:
                file.write(fragment[constants.HEADER_SIZE:])
            file.close()
        print("Cesta k ulozenemu suboru: server/" + file_name)
    server_logout(server_sock, addr)


# handles data receiving on server side
def receive_data(server_sock, packet_amount, addr):
    received_packets = 0
    fragments = []
    server_sock.settimeout(3)
    # listen until all fragments are received
    while received_packets < packet_amount:
        try:
            fragment = server_sock.recv(constants.BUFFER_SIZE)
        except socket.timeout:
            missing = get_missing_packet_seq_nums(fragments, packet_amount)
            missing = missing[:len(missing) - 1]
            print("Chybajuce fragmenty", missing)
            server_sock.sendto(struct.pack("iIc", 0, 0, str.encode("M", constants.CODING)) +
                               str.encode(missing, constants.CODING), addr)
            continue

        if is_corrupted(fragment):
            print("Fragment", get_seq_num(fragment), "poskodeny")
            server_sock.sendto(struct.pack("iIc", get_seq_num(fragment), 0, str.encode("R", constants.CODING)), addr)
        else:
            print("Fragment", get_seq_num(fragment), "prijaty")
            fragments.append(fragment)
            received_packets += 1

    # all packets were received, send ACK
    server_sock.sendto(struct.pack("iIc", 0, 0, str.encode("F", constants.CODING)), addr)
    return fragments


# determines whether a fragment is corrupted by comparing fcs
def is_corrupted(fragment):
    client_fcs = struct.unpack("iIc", fragment[:constants.HEADER_SIZE])[1]
    server_fcs = zlib.crc32(fragment[constants.HEADER_SIZE:])
    return client_fcs != server_fcs


# returns sequence number of a fragment
def get_seq_num(fragment):
    return struct.unpack("i", fragment[:4])[0]


def get_missing_packet_seq_nums(packets, packet_amount):
    received_packet_nums = set()
    missing_packet_nums = ""
    for packet in packets:
        received_packet_nums.add(get_seq_num(packet))
    x = 0
    while x < packet_amount:
        if len(missing_packet_nums) > 1485:  # message should not be longer than max frag size
            break
        if x not in received_packet_nums:
            missing_packet_nums += str(x) + ";"
        x += 1
    return missing_packet_nums


# ----- user -----
def server_logout(server_sock, addr):
    while True:
        choice = input("1 - pokracovat v pocuvani na aktualnom porte\n2 - odhlasit sa\n3 - vymena roli\n")
        if choice == "1":
            server_login(server_sock)
            return
        elif choice == "2":
            server_sock.close()
            return
        elif choice == "3":
            client_login(server_sock, addr)
            return


def client_logout(client_sock, addr):
    while True:
        choice = input("1 - poslat novu spravu\n2 - odhlasit sa\n3 - vymena roli\n")
        if choice == "1":
            client_login(client_sock, addr)
            return
        elif choice == "2":
            client_sock.close()
            return
        elif choice == "3":
            server_login(client_sock)
            return


def user_dialog():
    while True:
        print("\n1 - klient\n2 - server\n3 - koniec")
        choice = input()
        if choice == "1":
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            ip_addr = input("Zadaj IP adresu servera:\n")
            port = int(input("Zadaj port servera:\n"))
            server_addr = (ip_addr, port)
            client_login(client_sock, server_addr)
        elif choice == "2":
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            ip = socket.gethostbyname(socket.gethostname())
            print(ip)
            port = int(input("Zadaj port\n"))
            pair = (ip, port)
            server_sock.bind(pair)
            server_login(server_sock)
        elif choice == "3":
            return


if __name__ == '__main__':
    user_dialog()
