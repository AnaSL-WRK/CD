"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
import json
import pickle
from queue import LifoQueue, Empty
import socket
from typing import Any, Tuple
import xml.etree.ElementTree as XML
from src.broker import Serializer

class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.topic = topic
        self.type = _type
        self.host = "localhost"
        self.port = 5000
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)##
        self.sock.connect((self.host, self.port))



    def push(self, value):  #✔️
        """Sends data to broker."""
        self.sock.send(value)



    def pull(self) -> Tuple[str, Any]:  #✔️
        """Receives (topic, data) from broker.
        Should BLOCK the consumer!"""

        header = self.sock.recv(1)  
        size_data = self.sock.recv(2)
        if not size_data:
            return None
        size = int.from_bytes(size_data, 'big')
        data = self.sock.recv(size)
        
        return data



    def list_topics(self, callback: Callable):  #✔️
        """Lists all topics available in the broker."""
        message = json.dumps({"command": "list_topics"}).encode('utf-8')
        self.sock.send(len(message).to_bytes(2, "big") + message)


    def cancel(self): #✔️
        """Cancel subscription."""
        message = json.dumps({"command": "unsubscribe", "topic": self.topic}).encode('utf-8')
        self.sock.send(len(message).to_bytes(2, "big") + message)



class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        super().__init__(topic, _type)
        self.serializer = Serializer.JSON
            	
        if _type == MiddlewareType.CONSUMER:
            header = Serializer.JSON.value.to_bytes(1, 'big')
            message = json.dumps({"command": "subscribe", 
                                  "topic": topic})
            size = len(message).to_bytes(2, "big")
            encoded_message = message.encode('utf-8')
            super().push(header + size + encoded_message)



    def push(self, value):  #✔️
        """Sends data to broker."""

        message = json.dumps({
            "command": "publish",
            "topic": self.topic,
            "data": value,
            "serializer": "JSON"
        }).encode('utf-8')

        header = self.serializer.value.to_bytes(1, 'big')
        size = len(message).to_bytes(2, "big")
        full_message = header + size + message
        super().push(full_message)



    def pull(self) -> Tuple[str, Any]:  #✔️
        """Receives (topic, data) from broker.
        Should BLOCK the consumer!"""
        data = super().pull()
        if data:
            decoded_data = json.loads(data)
            return decoded_data["topic"], decoded_data["data"]
        return None, None





class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.serializer = Serializer.XML

        if _type == MiddlewareType.CONSUMER:
                root = XML.Element('root')
                XML.SubElement(root, 'command').text = "subscribe"
                XML.SubElement(root, 'topic').text = self.topic
                messageEncoded = XML.tostring(root)

                header = self.serializer.value.to_bytes(1, 'big')
                size = len(messageEncoded).to_bytes(2, "big")
                full_message =  header + size + messageEncoded
                super().push(full_message)


    def push(self, value):      #✔️
        root = XML.Element('root')
        XML.SubElement(root, 'command').text = "publish"
        XML.SubElement(root, 'topic').text = self.topic
        XML.SubElement(root, 'data').text = str(value)
        XML.SubElement(root, 'serializer').text = "XML"
        messageEncoded = XML.tostring(root)

        
        header = self.serializer.value.to_bytes(1, 'big')
        size = len(messageEncoded).to_bytes(2, "big")
        full_message =  header + size + messageEncoded
        super().push(full_message)
    



    def pull(self) -> Tuple[str, Any]:
        """Receives (topic, data) from broker, expects data in XML format."""
        data = super().pull()
        if data:
            try:
                root = XML.fromstring(data)
                elements = {}
                for child in root:
                    elements[child.tag] = child.text
                return elements["topic"], elements["data"]
     
            except Exception as e:
                print(f"Error parsing XML: {e} {elements}")
                return None, None


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.serializer = Serializer.PICKLE
        if _type == MiddlewareType.CONSUMER:
            message = pickle.dumps({"command": "subscribe", "topic": topic})
            header = self.serializer.value.to_bytes(1, 'big')
            size = len(message).to_bytes(2, "big")
            full_message = header + size  + message
            super().push(full_message)




    def push(self, value):      #✔️
        """Sends data to broker."""
        message = pickle.dumps({
            "command": "publish",
            "topic": self.topic,
            "data": value,
            "serializer": "PICKLE"
        })

        header = self.serializer.value.to_bytes(1, 'big')
        size = len(message).to_bytes(2, "big")
        full_message = header + size  + message
        super().push(full_message)


    def pull(self) -> Tuple[str, Any]:  #✔️
        """Receives (topic, data) from broker."""
        data = super().pull()
        if data:
            try:
                decoded_data = pickle.loads(data)
                return decoded_data["topic"], decoded_data["data"]
            except pickle.PickleError as e:
                    print(f"Failed to decode Pickle data: {e}")
        
        return None, None
