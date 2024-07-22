"""Protocol for chat server - Computação Distribuida Assignment 1."""
import json
from datetime import datetime
from socket import socket


class Message:
    """Message Type."""
    def __init__(self,type) -> None:
        self.type = type


    
class JoinMessage(Message):     #inheritance
    """Message to join a chat channel."""
    def __init__(self,channel) -> None:
        self.channel = channel
        self.type = "join"      #super().__init__("join")

    
    def __repr__(self) -> str:
        data = {"command": "join", "channel": self.channel}
        return json.dumps(data)


class RegisterMessage(Message):
    """Message to register username in the server."""
    def __init__(self,user) -> None:
        self.user = user
        self.type = "register"    #super().__init__("register")


    def __repr__(self) -> str:
        data = {"command": "register", "user": self.user}
        return json.dumps(data)
    
    
class TextMessage(Message):
    """Message to chat with other clients."""
    def __init__(self, message, channel=None) -> None:
        self.message = message
        self.channel = channel
        self.type = "message"    #super().__init__("message")

        #timestamp
        self.ts = int(datetime.now().timestamp())
    

    def __repr__(self) -> str:
        #if no channel is given, send to main
        if self.channel == None :
            data = {"command": "message", "message": self.message, "ts":self.ts}
            return json.dumps(data)
        else:
            data = {"command": "message", "message": self.message, "channel":self.channel, "ts":self.ts}
            return json.dumps(data)


class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage object."""
        return RegisterMessage(username)


    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""
        return JoinMessage(channel)


    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""
        return TextMessage(message, channel)


    @classmethod
    def send_msg(cls, connection: socket, msg: Message):
        """Sends through a connection a Message object."""
        msg = str(msg)
        size = len(msg).to_bytes(2, "big")
        msgEncoded = msg.encode("utf-8")
        connection.send(size + msgEncoded)



    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        
        size = int.from_bytes(connection.recv(2), "big")

        if size == 0:
            return None
        
        msg_bytes = connection.recv(size)

        try:
            msg = json.loads(msg_bytes.decode("utf-8"))
        except json.JSONDecodeError:
            raise CDProtoBadFormat(msg_bytes)
        
        cmd = msg["command"]

        options = {
            "join": CDProto.join,
            "register": CDProto.register,
            "message": CDProto.message
        }

        if cmd not in options:
            raise CDProtoBadFormat(msg_bytes)

        # Now, handle each command accordingly
        if cmd == "message":
            message_text = msg["message"]
            channel = msg.get("channel")
            ts = msg.get("ts")

            
            if ts is not None:
                if channel is None:
                    return options[cmd](message_text)
                else:
                    return options[cmd](message_text, channel)
            else:
                raise CDProtoBadFormat(msg_bytes)

        elif cmd == "join":
            if "channel" not in msg:
                raise CDProtoBadFormat(msg_bytes)
            return options[cmd](msg["channel"])

        elif cmd == "register":
            if "user" not in msg:
                raise CDProtoBadFormat(msg_bytes)
            return options[cmd](msg["user"])




class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")
