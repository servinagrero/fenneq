#!/usr/bin/env python3

import json
import sys
from datetime import datetime

import pika
from pika.channel import Channel


class Listener:
    """Class to manage the communication from rabbitmq.

    Attributes:
        node_name: RabbitMQ routing key to listen to messages
        config: Dictionary containing internal configuration
    """

    def __init__(self, url, exchange_name, node_name: str):
        self.url = url
        self.node_name = node_name
        self.exchange_name = exchange_name
        self.queue_commands = None
        self.handlers = []
        self.middleware = []

    def __setup_broker(self):
        """Setup RabbitMQ to listen to messages.

        Args:
            None

        Returns:
            None
        """
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

    def on(self, handle: str, **options):
        """Register a function on a handle

        The same function can be added to multiple dispatchers and to different handlers.

        Example:

            @app2.on("foo")
            @app.on({"foo": "bar"})
            @app.on("baz")
            def hello()
                return "hello world"

        Args:
            handle: Pattern to match from the message received.

        Returns:
            None

        Todo:
            * Check that the sigature of the handler is correct.
            * Proper pattern matching on handle. Allow to put a schema.
            * Handle one_shot. After being called once, it will not be able to call it again.
        """

        def decorator(callback, *args, **kwargs):
            self.handlers.append((handle, callback, options))
            return callback

        return decorator

    def use_middleware(self, middleware):
        """Add a middleware to the dispatcher.

        Args:
            middleware: Function to act as middleware.

        Returns:
            None

        Todo:
            * Check that the sigature of the middleware is correct
        """
        self.middleware.append(middleware)

    def __match(self, handler, msg):
        """Match a handler to the message received.

        Args:
            handler: Handler to match against.
            msg: Message from RabbitMQ after all the middleware.
        Returns:
            True if the pattern matches
        """
        if not type(handler) is type(msg):
            return False
        if isinstance(handler, dict):
            return all(handler[key] == msg.get(key, None) for key in handler)
        if isinstance(handler, str):
            return handler in msg
        if isinstance(handler, bool):
            return handler

    def handle_message(self, channel: Channel, method_frame, props, body):
        """Dispatch the correct route when a message is received.

        Args:
            channel:
            method_frame:
            props:
            body:

        Returns:
            None
        """
        try:
            for middleware in self.middleware:
                channel, method_frame, props, body = middleware(
                    channel, method_frame, props, body
                )

            for name, fn, options in self.handlers:
                if self.__match(name, body):
                    fn(channel, method_frame, props, body)
                    if options.get("one_shot", False):
                        self.handlers.remove((name, fn, options))

        except Exception as e:
            print(f"Exception while handling message: {e}")

        finally:
            return None

    def run(self):
        """
        Listen to messages and dispatch operators.

        Args:
            None

        Todo:
            * Handle restarting the app in a loop.
        """
        self.__setup_broker()

        self.channel.basic_consume(
            queue=self.queue_commands,
            on_message_callback=self.handle_message,
            auto_ack=True,
        )
        self.channel.start_consuming()
        self.con.close()
