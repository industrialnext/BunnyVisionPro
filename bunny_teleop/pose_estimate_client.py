import pickle
import queue
import threading

import zmq
from tornado import ioloop
from zmq.eventloop import zmqstream


class PoseEstimateClient:
    def __init__(self, port: int, host="localhost"):
        # Set up the ZMQ context and socket
        self.ctx = zmq.Context()
        self.sub_bind_to = f"tcp://{host}:{port}"
        self.sub_socket = self.ctx.socket(zmq.SUB)
        self.sub_socket.connect(self.sub_bind_to)
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, b"")

        # Command buffer queue
        self.command_queue = queue.Queue()

        # Setting up the IO loop in a separate thread
        self._thread = threading.Thread(target=self.run)
        self._thread.daemon = True
        self._thread.start()

    def run(self):
        """Run the IOLoop to handle incoming ZMQ messages."""
        loop = ioloop.IOLoop.current()
        stream = zmqstream.ZMQStream(self.sub_socket, io_loop=loop)
        stream.on_recv(self.handle_command)
        loop.start()

    def handle_command(self, message):
        """Deserialize and enqueue the received command."""
        command = pickle.loads(message[0])
        self.command_queue.put(command)

    def get_command(self):
        """Fetch and return the latest command from the buffer."""
        try:
            # Return the next item from the queue, block if necessary until an item is available
            return self.command_queue.get_nowait()
        except queue.Empty:
            return None  # Return None if no command is available

    def stop(self):
        """Stop the IOLoop and close the socket."""
        ioloop.IOLoop.current().stop()
        self.sub_socket.close()
        self.ctx.term()