#!/usr/bin/env python3

import inspect
import json
from enum import Enum
from typing import Any, Callable, Dict, Union

import pika
from pika.channel import Channel


def match_handler(handler: Any, msg: Any):
    """Match a handler to the message received.

    Args:
        handler: Handler to match against.
        msg: Message from RabbitMQ after all the middleware.

    Returns:
        True if the pattern matches
    """
    if handler is True:
        return True
    if isinstance(handler, str):
        return handler in msg
    if isinstance(handler, type):
        return isinstance(msg, handler)
    if isinstance(handler, dict):
        return all(
            handler[key] == msg.get(key, None)
            or isinstance(msg.get(key, None), handler[key])
            for key in handler
        )
    return False


class MsgType(Enum):
    #: Raw bytes
    RAW = "RAW"
    #: JSON string
    JSON = "JSON"


class Agent:
    """Class to manage the communication from rabbitmq.

    Attributes:
        url: URL to connect to RabbitMQ
        exchange_name: RabbitMQ exchange to listen to commands
        node_name: RabbitMQ routing key to listen to messages
        queue_commands: Internal queue to send and received commands.
        handlers: List of handlers and their functions to execute.
        middleware: List of middleware to execute when a message is received.
        msg_type: Whether to send the message as bytes or in JSON
    """

    def __init__(self, url: str, exchange_name: str, node_name: str, msg_type: MsgType):
        self.url = url
        self.node_name = node_name
        self.exchange_name = exchange_name
        self.queue_commands = None
        self.handlers = []
        self.middleware = []
        self.msg_type = msg_type

        self.__setup_broker()

    def __setup_broker(self):
        """Setup RabbitMQ to listen to messages."""
        params = pika.URLParameters(self.url)
        self.con = pika.BlockingConnection(params)
        self.channel = self.con.channel()
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="topic",
            durable=False,
        )
        result = self.channel.queue_declare(queue="", exclusive=True)
        self.queue_commands = result.method.queue

        self.channel.queue_bind(
            exchange=self.exchange_name,
            queue=self.queue_commands,
            routing_key=self.node_name,
        )

    def on(self, handle: Any, **options: Dict):
        """Register a function on a handle.

        Args:
            handle: Handler to match the message to.
            options: Additional options for the handler

        Returns:
            None

        The same function can be added to multiple dispatchers and to different handlers.

        .. code-block:: python

            @app2.on("foo")
            @app.on({"foo": "bar"})
            @app.on("baz")
            def hello()
                return "hello world"

        .. todo::

            * Check that the sigature of the handler is correct.
            * Proper pattern matching on handle. Allow to put a schema.
        """

        def decorator(callback):
            self.handlers.append((handle, callback, options))
            return callback

        return decorator

    def use_middleware(self, middleware: Callable):
        """Add a middleware to the dispatcher.

        Args:
            middleware: Function to act as middleware.

        Returns:
            None
        """
        sig = inspect.signature(middleware)
        if len(sig.parameters) < 4:
            print(f"Middleware {middleware.__name__} should have 4 arguments.")
        else:
            self.middleware.append(middleware)

    def handle_message(self, channel: Channel, method_frame, props, body):
        """Dispatch the correct route when a message is received.

        Args:
            channel: Channel the message was received from.
            method_frame: Method used to pass the message.
            props: Properties of the message.
            body: Message received from rabbitmq.

        Returns:
            None
        """
        try:
            for middleware in self.middleware:
                channel, method_frame, props, body = middleware(
                    channel, method_frame, props, body
                )

            if self.msg_type == MsgType.JSON:
                msg_body = json.loads(body)
            else:
                msg_body = body

            for name, handler_fn, options in self.handlers:
                if match_handler(name, msg_body):
                    handler_fn(channel, method_frame, props, msg_body)
                    if options.get("one_shot", False):
                        self.handlers.remove((name, handler_fn, options))

        except Exception as excep:
            print(f"Exception while handling message: {excep}")

    def send(self, msg: Union[Dict, str, bytes]):
        """Send a message to the running agent.

        Args:
            msg: message to be sent to the agent.

        Returns:
            None

        .. code-block:: python

            app.send(json.dumps({"message": "Hello there"}))
        """
        if self.msg_type == MsgType.JSON:
            msg = json.dumps(msg).encode("utf-8")

        self.channel.basic_publish(
            exchange=self.exchange_name, routing_key=self.node_name, body=msg
        )

    def run(self):
        """
        Listen to messages and dispatch handlers.

        Raises:
            RuntimeError: No handlers have been attached to the agent.

        Returns:
            None

        .. todo::

            * Handle restarting the app in a loop.
        """
        if not self.handlers:
            raise RuntimeError("There are no handlers attached.")

        try:
            self.channel.basic_consume(
                queue=self.queue_commands,
                on_message_callback=self.handle_message,
                auto_ack=True,
            )
            self.channel.start_consuming()

        except KeyboardInterrupt:
            print("CTRL-C pressed. Exiting")
            self.con.close()
        except Exception as excep:
            print(f"Exception at runtime: {excep}")
            self.con.close()
