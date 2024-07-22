"""Message Broker"""
import enum
import json
import pickle
import select
import selectors
import socket
from typing import Dict, List, Any, Tuple
import xml.etree.ElementTree as XML




class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self.host = "localhost"
        self.port = 5000


        self.selector = selectors.DefaultSelector()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('localhost', 5000))
        self.server_socket.listen(20)
        self.selector.register(self.server_socket, selectors.EVENT_READ, self.accept_connection)

        self.clients = {}
        self.topics = {}
        self.subscriptions = {}


    def accept_connection(self, sock, mask):
        """Accepts a new connection"""
        client_socket, addr = sock.accept()
        print(f"Connected by {addr}")
        self.selector.register(client_socket, selectors.EVENT_READ, self.handle_client)


    def disconnect_client(self, client_socket):
        """Disconnect client."""
        print(f"Client {client_socket.getpeername()} disconnected.")
        self.remove_client(client_socket)
        self.selector.unregister(client_socket)
        client_socket.close()



    def handle_client(self, client_socket, mask):
        try:
            header = client_socket.recv(1)
            if not header:
                raise ValueError("No serializer header received")
            
            serializer_type = Serializer(header[0])
            print(f"Received serializer type: {serializer_type.name}")

            size_bytes = client_socket.recv(2)

            if not size_bytes:
                raise ValueError("No size header received")
            
            size = int.from_bytes(size_bytes, "big")
            message = client_socket.recv(size)


            if not message:
                raise ValueError("No message received")
            
            self.process_request(message, client_socket, serializer_type)

        except ValueError as e:
            print(f"Error: {e}")
            self.disconnect_client(client_socket)

        except KeyError:
            print(f"Invalid serializer received: {header[0]}")
            self.disconnect_client(client_socket)




    def process_request(self, data, client_socket, serializer_type):
        """Process incoming requests from clients."""

        message = self.deserialize_data(data, serializer_type)

        if not message:
            return
        
        command = message.get('command')

        if not command:
            print("Invalid command received.")
            return
        

        try:
            topic = message.get('topic')
            msgData = message.get('data')

            #subscribe to a topic
            if command == 'subscribe':
                self.subscribe(topic, client_socket, serializer_type)
                print(f"Client {client_socket.getpeername()} subscribed to {topic} with {serializer_type.name} format.")


            #unsubscribe from a topic
            elif command == 'unsubscribe':
                self.unsubscribe(topic, client_socket)
                print(f"Client {client_socket.getpeername()} unsubscribed from {topic}.")


            #publish a message to a topic
            elif command == 'publish':
                self.put_topic(topic, msgData)
                message = {
                        "command": "publish",
                        "topic": topic,
                        "data": msgData,
                        "serializer": "JSON"
                    }
                self.publish(topic, message, serializer_type)
                print(f"Message published to {topic}: {data}")


            #list all topics
            elif command == 'list_topics':
                topics = self.list_topics()
                client_socket.send(json.dumps({'topics': topics}).encode('utf-8'))
                print("Sent list of topics to client.")

            else:
                print(f"Received unknown command: {command}")

        except Exception as e:
            print(f"Error processing request from {client_socket.getpeername()}: {e}")


    def list_topics(self) -> List[str]: #✔️
        """Returns a list of strings containing all topics containing values."""
        #return list(self.subscriptions.keys())
        return list(self.topics.keys())
    

    def get_topic(self, topic): #✔️
        """Returns the currently stored value in topic."""
        #return self.subscriptions.get(topic, None)
        if topic in self.topics:
            return self.topics[topic]
    

    def put_topic(self, topic, value):  #✔️
        """Store in topic the value."""
        self.topics[topic] = value    
        print(f"Data for topic {topic} updated. Value: {value}")

        
    def serialize_data(self, data, serializer_type):
        if serializer_type == Serializer.JSON:
            return json.dumps(data).encode("utf-8")
        
        elif serializer_type == Serializer.XML:
            root = XML.Element('root')
            for key, value in data.items():
                elem = XML.SubElement(root, key)
                elem.text = str(value)
            return XML.tostring(root)
        
        elif serializer_type == Serializer.PICKLE:
            return pickle.dumps(data)


    def deserialize_data(self, data, serializer_type):
        try:
        
            if serializer_type == Serializer.JSON:
                return json.loads(data)
            
            elif serializer_type == Serializer.XML:
                    root = XML.fromstring(data)

                    elements = {}
                    for child in root:
                        elements[child.tag] = child.text

                    return elements
                
            elif serializer_type == Serializer.PICKLE:
                return pickle.loads(data)
            else:
                raise ValueError(f"Unknown serializer: {serializer_type}")
        except Exception as e:
            print(f"Error deserializing data: {e}, {data}")
            return None
        

    def publish(self, topic, data, serializer_type=Serializer.JSON):
        notified_clients = set()  #avoid sending duplicates

        for subscribed_topic, clients in self.subscriptions.items():
            if subscribed_topic == topic or topic.startswith(subscribed_topic + '/'):
                for client, ser in clients:
                    if client not in notified_clients:
                        try:
                            serialized_data = self.serialize_data(data, ser)
                            header = ser.value.to_bytes(1, 'big')
                            size = len(serialized_data).to_bytes(2, "big")
                            client.send(header + size + serialized_data)
                            notified_clients.add(client)
                            print(f"Message sent to {client.getpeername()} on topic {subscribed_topic}.")
                        except Exception as e:
                            print(f"Failed to send data to {client.getpeername()}: {e}")


    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:     #✔️
        """Provide list of subscribers to a given topic."""
        return self.subscriptions.get(topic, [])


    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address and all its subtopics."""

        #topic and its subtopics are subscribed
        if topic not in self.subscriptions:
            self.subscriptions[topic] = []
        self.subscriptions[topic].append((address, _format))
        
        #add subscription to all subtopics of the current topic
        for subtopic in list(self.subscriptions.keys()):
            if subtopic.startswith(topic + "/"):
                if (address, _format) not in self.subscriptions[subtopic]:
                    self.subscriptions[subtopic].append((address, _format))

    



    def unsubscribe(self, topic, client_socket):
        if topic in self.subscriptions:
            self.subscriptions[topic] = [(sock, ser) for sock, ser in self.subscriptions[topic] if sock != client_socket]
            if not self.subscriptions[topic]:
                del self.subscriptions[topic]



    def remove_client(self, client_socket):
        """Remove the client from all subscriptions."""
        for topic, subscribers in list(self.subscriptions.items()):
            self.subscriptions[topic] = [(sock, ser) for sock, ser in subscribers if sock != client_socket]

            if not self.subscriptions[topic]:
                del self.subscriptions[topic]

        print(f"Removed client {client_socket.getpeername()} from all subscriptions.")
    
    
    def run(self):
        """Run until canceled."""
        print("Broker server is running.")

        clients = []
        try:

            while not self.canceled:
                events = self.selector.select(timeout=None)
                for key, mask in events:
                    callback = key.data
                    callback(key.fileobj, mask)

        except KeyboardInterrupt:
            #catch keyboardInterrupt and exit
            print("Broker is shutting down due to a KeyboardInterrupt.")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            self.server_socket.close()
            for client in clients:
                client.close()
            print("All connections were closed.")