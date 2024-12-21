import os
import socket
import subprocess
import threading
import time

# Constants
NODE_IP = '0.0.0.0'  # Listen on all interfaces
NODE_PORT = 5000
BROADCAST_IP = '255.255.255.255'
BROADCAST_PORT = 10000
WORK_FOLDER = 'work'
OUTPUT_FOLDER = 'output'
FFMPEG_CMD_TEMPLATE = 'ffmpeg -i {} -c:v libx265 -preset slow -crf 28 -c:a copy {}'

# Create work and output folders if they don't exist
os.makedirs(WORK_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def handle_client(conn, addr):
    print(f'Connected by {addr}')
    while True:
        data = conn.recv(1024).decode()
        if not data:
            break
        job = data.strip()
        if job:
            print(f'Received job: {job}')
            input_path = os.path.join(WORK_FOLDER, job)
            output_path = os.path.join(OUTPUT_FOLDER, f'converted_{job}')
            convert_video(input_path, output_path, conn)

def convert_video(input_path, output_path, conn):
    try:
        command = FFMPEG_CMD_TEMPLATE.format(input_path, output_path)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while True:
            output = process.stdout.readline()
            if output == b'' and process.poll() is not None:
                break
            if output:
                conn.sendall(output)
        rc = process.poll()
        if rc == 0:
            conn.sendall(b'done')
            print(f'Converted {input_path} to {output_path}')
    except Exception as e:
        print(f'Error during conversion: {e}')
        conn.sendall(b'error')

def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((NODE_IP, NODE_PORT))
        s.listen()
        print(f'Slave node listening on {NODE_IP}:{NODE_PORT}')
        while True:
            conn, addr = s.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()

def listen_for_broadcast():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.bind((NODE_IP, BROADCAST_PORT))
        while True:
            data, addr = s.recvfrom(1024)
            if data == b'NODE_SCAN':
                print('Received broadcast request')
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as response_socket:
                    response_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                    response = f'NODE_RESPONSE {socket.gethostbyname(socket.gethostname())} {NODE_PORT}'
                    response_socket.sendto(response.encode(), (BROADCAST_IP, BROADCAST_PORT))

if __name__ == '__main__':
    threading.Thread(target=listen_for_broadcast).start()
    start_server()
