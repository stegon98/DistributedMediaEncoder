import os
import socket
import subprocess
import threading
from queue import Queue
import time

# Constants
NUM_NODES = 2
WORK_FOLDER = 'work'
OUTPUT_FOLDER = 'output'
VIDEO_FILE = 'input_video.mp4'
FFMPEG_CMD_TEMPLATE = 'ffmpeg -i {} -c:v libx265 -preset fast -crf 20 -c:a copy {}'

# Create work and output folders if they don't exist
os.makedirs(WORK_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def scan_for_nodes():
    nodes = []
    # Placeholder for actual network scanning logic
    for i in range(NUM_NODES):
        node_ip = f'192.168.1.{100 + i}'
        node_port = 5000 + i
        nodes.append((node_ip, node_port))
    return nodes

def split_video(file_path, segment_length=60):
    # Placeholder for actual video splitting logic using ffmpeg
    command = f'ffmpeg -i {file_path} -c copy -map 0 -segment_time {segment_length} -f segment -reset_timestamps 1 {WORK_FOLDER}/video_%03d.mp4'
    subprocess.run(command, shell=True)

def distribute_jobs(nodes):
    jobs = Queue()
    for file in sorted(os.listdir(WORK_FOLDER)):
        if file.endswith('.mp4'):
            jobs.put(file)

    threads = []
    for node_ip, node_port in nodes:
        t = threading.Thread(target=handle_node, args=(node_ip, node_port, jobs))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

def handle_node(node_ip, node_port, jobs):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((node_ip, node_port))
        while not jobs.empty():
            job = jobs.get()
            print(f'Sending {job} to {node_ip}:{node_port}')
            s.sendall(job.encode())
            response = s.recv(1024).decode()
            if 'done' in response:
                print(f'Received {response} from {node_ip}:{node_port}')
            else:
                print(f'Error: {response}')
            jobs.task_done()

def concatenate_videos():
    # Placeholder for actual video concatenation logic using ffmpeg
    command = f'ffmpeg -f concat -safe 0 -i <(for f in {WORK_FOLDER}/*.mp4; do echo "file \'$f\'"; done) -c copy {OUTPUT_FOLDER}/output_video.mp4'
    subprocess.run(command, shell=True)

def main():
    nodes = scan_for_nodes()
    split_video(VIDEO_FILE)
    distribute_jobs(nodes)
    concatenate_videos()

if __name__ == '__main__':
    main()
