"""CD Chat server program."""
import logging
import selectors
import socket

from src.protocol import CDProto, CDProtoBadFormat

logging.basicConfig(filename="server.log", level=logging.DEBUG)

#init server and listen for connections
#read msgs and broadcast to all clients if on the same channel
#register /JOINS and exits
#log all messages


class Server:
    """Chat Server process."""
    def __init__(self, host="127.0.0.1", port=8888):
        self.host = host
        self.port = port

        self.channels = {"main": []}
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        self.server.bind((self.host, self.port))
        self.server.listen()        #max connections

        self.selector = selectors.DefaultSelector()
        self.selector.register(self.server, selectors.EVENT_READ, self.accept_connection)        #new connection
    



    def loop(self):
        """Loop indefinetely."""
        while True:
            events = self.selector.select()     #wait for events
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)



    def accept_connection(self, server_socket, mask):
        """Accept a new connection."""
        conn, addr = server_socket.accept()
        logging.info(f"{addr} connected to the server.")
        conn.setblocking(False)
        self.channels["main"].append(conn)
        self.selector.register(conn, selectors.EVENT_READ, self.handle_client)



    def handle_client(self, conn,mask):
        """Handle communication with a connected client."""
        try:
            #get all msgs
            message = CDProto.recv_msg(conn)
            logging.debug('received %s', message)

            if message != None:
                command = message.type

                if message.type == 'register':
                    return

            
                options = {
                    "join": self.handle_join,
                    "message": self.handle_message,
                }
                options[command](conn, message)


            #no msg, client disconnected
            else:
                logging.info(f"{conn.getpeername()} disconnected from the server.")

                #remove client from channels
                for channel in self.channels.keys():
                    if conn in self.channels[channel]:
                        self.channels[channel].remove(conn)

                self.selector.unregister(conn)
                conn.close()

        #bad format
        except CDProtoBadFormat:
            print("Message in bad format")
        

        except Exception as e:
            logging.error(f"Error handling client {conn.getpeername()}: {e}")
            self.selector.unregister(conn)
            conn.close()

    def handle_join(self, conn, message):
        new_channel = message.channel
        
        # Remove the connection from all channels except the new one
        for channel in self.channels.keys():
            if conn in self.channels[channel] and channel != new_channel:
                self.channels[channel].remove(conn)

        # If the new channel does not exist, create it
        if new_channel not in self.channels:
            self.channels[new_channel] = []

        # Add the connection to the new channel if not already present
        if conn not in self.channels[new_channel]:
            self.channels[new_channel].append(conn)


    def handle_message(self, conn, message):
        #send msg to all clients in the channel
        for client in self.channels[message.channel]:
            CDProto.send_msg(client, message)
            logging.debug('sent "%s', message)
