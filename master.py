import socket
import os
from multiprocessing import Pool
from subprocess import Popen, PIPE
import queue
import time

# Define a function to benchmark the computational power of a node
def benchmark(node):
    # Send a sample file to the node and measure the time it takes to complete
    with open("sample.mp4", "rb") as f:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((node, 12345))
        sock.sendall(f.read())

    # Measure the time it took to send and process the sample file
    start_time = time.time()
    os.system(f"ffmpeg -i {f.name} -c:v h265 -crf 18 output.mp4")
    end_time = time.time()

    # Return the computational power of the node
    return 1 / (end_time - start_time)

# Define a function to split the video into segments and send them to nodes
def split_video(file_name, num_nodes):
    # Split the video into 60-second segments
    command = f"ffmpeg -i {file_name} -c:v h265 -crf 18 -segment_times 1,2,3,4,5,6,7,8,9,10 output%03d.mp4"
    os.system(command)

    # Get the number of segments
    num_segments = len(os.listdir("segments"))

    # Create a pool of worker processes
    with Pool(processes=num_nodes) as pool:
        # Map the benchmark function to each node and get their computational powers
        computational_powers = pool.map(benchmark, nodes)

    # Assign more jobs to faster nodes and fewer jobs to slower nodes
    assignments = {}
    for i in range(num_segments):
        assignments[i] = nodes[np.argmin([abs(computational_powers[j] - 1) for j in range(len(nodes))])]

    return assignments

# Define a function to send segments to nodes and wait for completion
def send_segments(assignments, file_name, num_nodes):
    # Create a queue to store the completed segments
    queue = queue.Queue()

    # Create a pool of worker processes
    with Pool(processes=num_nodes) as pool:
        # Map the encode function to each node and get their completion percentages
        completion_percentages = pool.starmap(encode, [(assignments[i], i, file_name) for i in range(len(assignments))])

    # Get the completed segments from the queue
    completed_segments = []
    while not queue.empty():
        completed_segments.append(queue.get())

    return completed_segments

# Define a function to concatenate the completed segments
def concatenate_segments(completed_segments):
    # Create an output file name
    output_file_name = "output.mp4"

    # Concatenate the completed segments
    command = f"ffmpeg -f concat -i temp.txt -c copy {output_file_name}"
    with open("temp.txt", "w") as f:
        for segment in completed_segments:
            f.write(f"file '{segment}'\n")
    os.system(command)

    # Remove the temporary files
    os.remove("temp.txt")

# Define a function to encode a segment on a node
def encode(node, index, file_name):
    # Connect to the node and send the segment
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((node, 12345))
    with open(f"segments/output{index}.mp4", "rb") as f:
        sock.sendall(f.read())

    # Wait for the node to complete encoding the segment
    completion_percentage = float(sock.recv(1024).decode("utf-8"))

    # Get the completed segment from the node
    completed_segment = b""
    while len(completed_segment) < os.path.getsize(f"segments/output{index}.mp4"):
        data = sock.recv(4096)
        completed_segment += data

    # Store the completed segment in a queue
    queue.put((completed_segment, index))

    return completion_percentage

# Define the number of nodes to use
num_nodes = 2

# Define the nodes to connect to
nodes = [f"node{i}" for i in range(num_nodes)]

# Get the video file name from the user
file_name = input("Enter the video file name: ")

# Split the video into segments and send them to nodes
assignments = split_video(file_name, num_nodes)

# Send the segments to nodes and wait for completion
completed_segments = send_segments(assignments, file_name, num_nodes)

# Concatenate the completed segments
concatenate_segments(completed_segments)
