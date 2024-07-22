"""CD Chat client program"""
import logging
import selectors
import socket
import sys
import threading

from src.protocol import CDProto,  RegisterMessage, JoinMessage, TextMessage


logging.basicConfig(filename=f"{sys.argv[0]}.log", level=logging.DEBUG)



class Client:
    """Chat Client process."""

    def __init__(self, name: str = "Foo"):
        """Initializes chat client."""

        self.name = name
        self.channel = "main"       #default channel

        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.selector = selectors.DefaultSelector()
      


    def connect(self):
        """Connect to chat server and start thread to read from user """
        self.client.connect(("127.0.0.1", 8888))
        self.client.setblocking(False)

        self.selector.register(self.client, selectors.EVENT_READ, self.handle_server_input)

        CDProto.send_msg(self.client,RegisterMessage(self.name))
        logging.debug(f'register %s', self.name)
      

    def loop(self):
        """Loop indefinetely."""
        threading.Thread(target=self.handle_user_input, daemon=True).start()
        while True:
            events = self.selector.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)


    def handle_server_input(self, conn, mask):
        channel_messages = CDProto.recv_msg(conn)

        if channel_messages != None:
            command = channel_messages.type

            if command == "message":
                print(channel_messages.message)
                logging.debug('received %s', channel_messages.message)



    def handle_user_input(self):
        while True:
            try: 
                message = input()

                #join
                if message.split()[0] == "/join":
                    self.channel = message.split()[1]

                    #send join msg
                    CDProto.send_msg(self.client,JoinMessage(self.channel))
                    logging.debug('sent "%s', message)

                # exit 
                elif message.lower() == "exit":
                    try:
                        self.selector.unregister(self.client)
                        self.client.shutdown(socket.SHUT_RDWR)
                    except OSError as e:
                        logging.error(f"Error during client shutdown: {e}")
                    finally:
                        self.client.close()
                        logging.info("Connection closed.")
                        sys.exit(0)

                #msg
                else:
                    CDProto.send_msg(self.client,TextMessage(message,channel=self.channel))
                    logging.debug('sent %s', message)

    
            except (socket.timeout, socket.error):
                print('[ERROR] Server timed out.')
                sys.exit(0)
            
