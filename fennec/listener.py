#!/usr/bin/env python3

import sys
import inspect
from typing import Any, Callable, Dict

import pika
from pika.channel import Channel


def match(handler: Any, msg: Any):
    """Match a handler to the message received.

    :param handler: Handler to match against.
    :type handler: Any

    :param msg: Message from RabbitMQ after all the middleware.
    :type msg: Any

    :return: True if the pattern matches
    :rtype: bool
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


class Listener:
    """Class to manage the communication from rabbitmq.

    :param url: URL to connect to RabbitMQ
    :type url: str

    :param exchange_name: RabbitMQ exchange to listen to commands
    :type exchange_name: str

    :param node_name: RabbitMQ routing key to listen to messages
    :type node_name: str

    :param queue_commands:
    :type queue_commands:

    :param handlers:
    :type handles:

    :param middleware:
    :type middleware: list[Callable]
    """

    def __init__(self, url: str, exchange_name: str, node_name: str):
        self.url = url
        self.node_name = node_name
        self.exchange_name = exchange_name
        self.queue_commands = None
        self.handlers = []
        self.middleware = []

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

        :param handle:
        :type handle:

        :param options:
        :type options:

        The same function can be added to multiple dispatchers and to different handlers.

        .. code-block:: python

            @app2.on("foo")
            @app.on({"foo": "bar"})
            @app.on("baz")
            def hello()
                return "hello world"

        :return: None

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

        :param middleware: Function to act as middleware.
        :type middleware: Callable

        :return: None

        .. todo::
            * Check that the sigature of the middleware is correct
        """
        sig = inspect.signature(middleware)
        if len(sig.parameters) < 4:
            print(f"Middleware {middleware.__name__} should have 4 arguments.")
        else:
            self.middleware.append(middleware)

    def handle_message(self, channel: Channel, method_frame, props, body):
        """Dispatch the correct route when a message is received.

        :param channel:
        :type channel: pika.channel.Channel

        :param method_frame:
        :type method_frame:

        :param props:
        :type props:

        :param body: message received from rabbitmq
        :type body: bytes

        :return: None
        """
        try:
            for middleware in self.middleware:
                channel, method_frame, props, body = middleware(
                    channel, method_frame, props, body
                )

            for name, handler_fn, options in self.handlers:
                if match(name, body):
                    handler_fn(channel, method_frame, props, body)
                    if options.get("one_shot", False):
                        self.handlers.remove((name, handler_fn, options))

        except Exception as excep:
            print(f"Exception while handling message: {excep}")

    def send(self, msg: bytes):
        """Send a message to the running agent.

        :param msg: message to be sent to the agent.
        :type msg: bytes
        
        .. code-block:: python

            app.send(json.dumps({"message": "Hello there"}))

        :return: None
        """
        self.channel.basic_publish(
            exchange=self.exchange_name, routing_key=self.node_name, body=msg
        )

    def run(self):
        """
        Listen to messages and dispatch handlers.

        :raise RuntimeError: No handlers have been attached to the agent.
        
        :return: None
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
            print(excep)
            self.con.close()
