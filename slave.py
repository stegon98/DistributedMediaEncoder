import os
import socket
import subprocess
import threading
import time

# Constants
NODE_IP = '192.168.0.101'  # Replace with actual IP address
NODE_PORT = 5000  # Replace with actual port number
WORK_FOLDER = 'work'
OUTPUT_FOLDER = 'output'
FFMPEG_CMD_TEMPLATE = 'ffmpeg -i {} -c:v libx265 -preset fast -crf 20 -c:a copy {}'

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

if __name__ == '__main__':
    start_server()
