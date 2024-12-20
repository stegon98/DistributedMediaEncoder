import socket
from subprocess import Popen, PIPE

# Define a function to wait for a node and tell the master that the node is ready to receive a job
def wait_for_node(master_ip):
    # Wait for the master to send a message indicating that it has assigned a job to this node
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((master_ip, 12345))

    # Get the segment name from the master
    segment_name = sock.recv(1024).decode("utf-8")

    return segment_name

# Define a function to encode a segment on this node
def encode_segment(segment_name):
    # Connect to the master and send the completion percentage
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("master_node_ip", 12345))
    with open(segment_name, "rb") as f:
        sock.sendall(f.read())

    # Wait for the master to send a message indicating that it has received the completed segment
    completion_percentage = float(sock.recv(1024).decode("utf-8"))

    return completion_percentage

# Define the function to run on each node
def run_node(num_nodes):
    # Wait for the master to assign a job to this node
    segment_name = wait_for_node(f"master_node_ip")

    # Encode the segment
    completion_percentage = encode_segment(segment_name)

    # Return the completion percentage
    return completion_percentage

# Define the function to send the completed segment back to the master
def send_completed_segment(segment_name):
    # Connect to the master and send the completed segment
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("master_node_ip", 12345))
    with open(segment_name, "rb") as f:
        sock.sendall(f.read())

# Define the IP address of the master node
master_node_ip = "192.168.1.100"

# Run the function on this node
run_node(4)
