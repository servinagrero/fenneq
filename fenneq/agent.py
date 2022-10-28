#!/usr/bin/env python3

from __future__ import annotations

import inspect
import json
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import sys

from pika import BlockingConnection, URLParameters

@dataclass
class Message:
    """Wrapper for a message received from RabbitMQ.

    Attributes:
        channel: Channel the message was received from.
        method: Method used to pass the message.
        props: Properties of the message.
        body: Message received from RabbitMQ.
    """

    channel: Any
    method: Any
    props: Any
    body: Any


# Callback function
Callback = Callable[[Message], None]

# Middleware to be executed before a function handler.
Middleware = Callable[[Message], Message]


def match_handler(handler: Any, msg: Any, strict: bool = True) -> bool:
    """Check if a message matches a user defined pattern.

    The matching is done with strict equality. This means that if msg is a dict, handler needs to have the same keys and same types for the values.

    Args:
        handler: Handler to match against.
        msg: Message from RabbitMQ after all the middleware.

    Returns:
        True if the pattern matches,
    """
    if handler is True:
        return True
    
    if isinstance(handler, type):
        return isinstance(msg, handler)

    if isinstance(handler, str) and isinstance(msg, str):
        return handler == msg

    if isinstance(handler, dict) and isinstance(msg, dict):
        if strict:
            keys_check = handler.keys() == msg.keys()
            if keys_check is False:
                return False
            return all(match_handler(h, v) for h,v in zip(handler.values(), msg.values()))
        
        return any(match_handler(h, v) for h,v in zip(handler.values(), msg.values()))
    
    return False


class BasicAgent:
    """Class for a basic agent.

    This class implements the basic functionality to connect to RabbitMQ.
    Subclasses should add the functionality to read or send messages.

    Args:
        url: URL to connect to RabbitMQ.
        name: Name of the Agent. Serves as routing key.
        exchange: Exchange name for RabbitMQ.

    Attributes:
        connection: Connection to RabbitMQ.
        channel: Connection channel.
        name: Name of the agent. Serves as routing key.
        exchange: RabbitMQ exchange name.
    """

    def __init__(self, url: str, name: str, exchange: str):
        parameters = URLParameters(url)
        self.connection = BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.name = name
        self.exchange = exchange

        self.declare_exchange()

    def declare_queue(self, queue_name: str = ""):
        """Declare a RabbitMQ queue.
        Args:
            queue_name: Name of the queue. Defaults to ''.
        """
        return self.channel.queue_declare(queue=queue_name, exclusive=True)

    def declare_exchange(self):
        """Declare a RabbitMQ exchange."""
        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type="topic",
            durable=False,
        )

    def send(
        self,
        msg: Union[Dict, str, bytes],
        to: Optional[str] = None,
        at: Optional[str] = None,
    ):
        """Send a message to an agent.

        Args:
            msg: message to be sent to the agent.
            to: Node name to send the message. Defaults to the assigned name.
            at: Exchange name to send the message. Defaults to the assigned exchange.

        Example:
            >>> agent.send("Hello there")
            >>> agent.send(b'Message in bytes')
            >>> agent.send({"message": 42})
            >>> agent.send({"message": 42}, to="another.agent", at="another_exchange")
        """
        body = json.dumps(msg).encode("utf-8")
        routing_key = to or self.name
        exchange = at or self.exchange

        if routing_key is None:
            raise ValueError("Name cannot be None when sending a message")

        if exchange is None:
            raise ValueError("Exchange cannot be None when sending a message")

        self.channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=body
        )

    def close(self):
        """Close the channel and connection"""
        self.channel.close()
        self.connection.close()


class Agent(BasicAgent):
    """Class to manage the communication from rabbitmq.

    Attributes:
        queue_commands: Internal queue to send and received commands.
        handlers: List of handlers and their functions to execute.
        middleware: List of middleware to execute right when a message is received.
    """

    def __init__(self, url: str, name: str, exchange: str):
        super(Agent, self).__init__(url, name, exchange)
        self.queue_commands = None
        self.handlers: List[Tuple[Any, Callback, Dict[str, bool]]] = []
        self.middleware: List[Middleware] = []

        self.declare_exchange()

        result = self.declare_queue()
        self.queue_commands = result.method.queue
        self.channel.queue_bind(
            exchange=self.exchange,
            queue=self.queue_commands,
            routing_key=self.name,
        )

    def on(self, handle: Any, **options):
        """Register a function on a handle.

        Args:
            handle: Handler to check against the received message.
            options: Additional options for the handler

        The same function can be added to multiple dispatchers and to different handlers.

        The following options are allowed:
        * oneshot: The handler only works once. Once the function is dispatched the handler is removed.

        Example:
            >>> @agent.on("baz", oneshot=True)
            >>> @agent.on({"foo": "bar"})
            >>> @agent2.on("foo")
            >>> def hello()
            >>>     return "hello world"

        Todo:
            * Proper pattern matching on handle. Allow to put a schema.
        """

        def decorator(callback):
            if len(inspect.signature(callback).parameters) != 1:
                raise ValueError(
                    f'Callback "{callback.__name__}" needs to have only one parameter'
                )

            @wraps(callback)
            def wrapper():
                self.handlers.append((handle, callback, options))
                return callback

            wrapper()

        return decorator

    def with_middleware(self, middleware):
        """Add a middleware to the dispatcher.

        A middleware receives a Message and returns a Message.
        If None is returned, no more middlewares are evaluated.

        Args:
            middleware: Function to act as middleware.
        """
        sig = inspect.signature(middleware)
        if len(sig.parameters) != 1:
            print(f"Middleware {middleware.__name__} needs only one parameter.")
        else:
            self.middleware.append(middleware)

    def get_message(self) -> Optional[Message]:
        method, props, body = self.channel.basic_get("")
        if method:
            return Message(self.channel, method, props, body)
        return None

    def handle_message(self, message: Message):
        """
        Options:
        - one_shot: The callback is removed from the list after being executed.
        - block: Do not check for more handlers, even if they can match.
        """

        try:
            for middleware in self.middleware:
                res = middleware(message)
                if res is None:
                    message = res
                    break
                message = res

            for name, handler_fn, options in self.handlers:
                if match_handler(name, message.body, options.get("strict", True)):
                    handler_fn(message)
                    if options.get("one_shot", False):
                        self.handlers.remove((name, handler_fn, options))
                    if options.get("block", False):
                        break
        except Exception as excep:
            print(f"Exception while handling message: {excep}")

    def message_callback(self, channel, method_frame, props, body: bytes):
        """Dispatch a callback function when a message is received.

        Args:
            channel: Channel the message was received from.
            method_frame: Method used to pass the message.
            props: Properties of the message.
            body: Message received from RabbitMQ.
        """
        msg_body = json.loads(body)
        message = Message(channel, method_frame, props, msg_body)
        self.handle_message(message)

    def run(self):
        """
        Listen to messages and dispatch handlers.

        Raises:
            RuntimeError: No handlers have been attached to the agent.

        Todo:
            * Handle restarting the app in a loop.
        """
        if not self.handlers:
            raise RuntimeError("There are no handlers attached.")

        try:
            self.channel.basic_consume(
                queue=self.queue_commands,
                on_message_callback=self.message_callback,
                auto_ack=True,
            )
            self.channel.start_consuming()

        except KeyboardInterrupt:
            print("CTRL-C pressed. Exiting")
            self.close()
        except Exception as excep:
            print(f"Exception at runtime: {excep}")
            self.close()


class Sender(BasicAgent):
    """Agent used to send messages."""

    def __init__(
        self, url: str, name: str = "", exchange: str = ""
    ):
        super(Sender, self).__init__(url, name, exchange)
