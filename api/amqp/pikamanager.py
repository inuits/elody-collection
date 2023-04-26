import itertools
import json

from datetime import datetime
from flask.app import Flask
from hashlib import sha256
from pika import spec
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from rabbitmq_pika_flask import RabbitMQ
from rabbitmq_pika_flask.ExchangeType import ExchangeType
from rabbitmq_pika_flask.QueueParams import QueueParams
from rabbitmq_pika_flask.RabbitConsumerMiddleware import (
    RabbitConsumerMessage,
    RabbitConsumerMiddleware,
    call_middlewares,
)
from rabbitmq_pika_flask.RabbitMQ import MessageErrorCallback
from retry import retry
from retry.api import retry_call
from threading import Thread
from typing import Callable, List, Union


class PikaAmqpManager(RabbitMQ):
    def __init__(self):
        super().__init__(queue_params=QueueParams(False, False, False))
        self.durable_exchange = False
        self.passive_exchange = False

    def init_app(
        self,
        app: Flask,
        queue_prefix: str,
        body_parser: Callable = lambda body: body,
        msg_parser: Callable = lambda msg: msg,
        development: bool = False,
        on_message_error_callback: Union[MessageErrorCallback, None] = None,
        middlewares: Union[List[RabbitConsumerMiddleware], None] = None,
    ):
        super().init_app(
            app,
            queue_prefix,
            body_parser,
            msg_parser,
            development,
            on_message_error_callback,
            middlewares,
        )
        self.durable_exchange = self.config["DURABLE_EXCHANGE"]
        self.passive_exchange = self.config["PASSIVE_EXCHANGE"]

    def _setup_connection(
        self,
        func: Callable,
        routing_key: Union[str, List[str]],
        exchange_type: ExchangeType,
        auto_ack: bool,
        dead_letter_exchange: bool,
        props_needed: List[str],
    ):
        """Setup new queue connection in a new thread

        Args:
            func (Callable): function to run as callback for a new message
            routing_key (str | list[str]): routing key(s) for the new queue bind
            exchange_type (ExchangeType): Exchange type to be used with new queue
            auto_ack (bool): If messages should be auto acknowledged.
            dead_letter_exchange (bool): If a dead letter exchange should be created for this queue
            props_needed (list[str]): List of properties to be passed along with body
        """

        def create_queue():
            return self._add_exchange_queue(
                func,
                routing_key,
                exchange_type,
                auto_ack,
                dead_letter_exchange,
                props_needed,
            )

        thread = Thread(target=create_queue, name=self._build_queue_name(func))
        thread.daemon = True
        thread.start()

    @retry((AMQPConnectionError, AssertionError), delay=5, jitter=(5, 15))
    def _add_exchange_queue(
        self,
        func: Callable,
        routing_key: Union[str, List[str]],
        exchange_type: ExchangeType,
        auto_ack: bool,
        dead_letter_exchange: bool,
        props_needed: List[str],
    ):
        """Creates or connects to new queue, retries connection on failure

        Args:
            func (Callable): function to run as callback for a new message
            routing_key (str | list[str]): routing key(s) for the new queue bind
            exchange_type (ExchangeType): Exchange type to be used with new queue
            auto_ack (bool): If messages should be auto acknowledged.
            dead_letter_exchange (bool): If a dead letter exchange should be created for this queue
            props_needed (list[str]): List of properties to be passed along with body
        """

        # Create connection channel
        connection = self.get_connection()
        channel = connection.channel()

        # declare dead letter exchange if needed
        if dead_letter_exchange:
            dead_letter_exchange_name = f"dead.letter.{self.exchange_name}"
            channel.exchange_declare(
                exchange=dead_letter_exchange_name,
                exchange_type=ExchangeType.DIRECT,
                durable=self.durable_exchange,
                passive=self.passive_exchange,
            )

        # Declare exchange
        channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type=exchange_type,
            durable=self.durable_exchange,
            passive=self.passive_exchange,
        )

        # Creates new queue or connects to existing one
        queue_name = self._build_queue_name(func)
        exchange_args = {}
        dead_letter_queue_name = None
        if dead_letter_exchange and not self.development:
            dead_letter_queue_name = f"dead.letter.{queue_name}"
            channel.queue_declare(
                dead_letter_queue_name,
                durable=self.queue_params.durable,
            )

            # Bind queue to exchange
            channel.queue_bind(
                exchange=dead_letter_exchange_name,
                queue=dead_letter_queue_name,
                routing_key=dead_letter_queue_name,
            )

            exchange_args = {
                "x-dead-letter-exchange": dead_letter_exchange_name,
                "x-dead-letter-routing-key": dead_letter_queue_name,
            }

        channel.queue_declare(
            queue_name,
            durable=self.queue_params.durable,
            auto_delete=self.queue_params.auto_delete,
            exclusive=self.queue_params.exclusive,
            arguments=exchange_args,
        )
        self.app.logger.info(f"Declaring Queue: {queue_name}")

        # Bind queue to exchange
        routing_keys = routing_key if isinstance(routing_key, list) else [routing_key]
        for routing_key in routing_keys:
            channel.queue_bind(
                exchange=self.exchange_name, queue=queue_name, routing_key=routing_key
            )

        def user_consumer(message: RabbitConsumerMessage, call_next) -> None:
            """User consumer as a middleware. Calls the consumer `func`."""
            func(
                routing_key=message.routing_key,
                body=message.parsed_body,
                **self.__get_needed_props(props_needed, message.props),
            )
            call_next(message)

        def callback(
            _: BlockingChannel,
            method: spec.Basic.Deliver,
            props: spec.BasicProperties,
            body: bytes,
        ):
            with self.app.app_context():
                decoded_body = body.decode()

                try:
                    # Fetches original message routing_key from headers if it has been dead-lettered
                    routing_key = method.routing_key

                    if getattr(props, "headers", None) and props.headers.get("x-death"):
                        x_death_props = props.headers.get("x-death")[0]
                        routing_key = x_death_props.get("routing-keys")[0]

                    message = RabbitConsumerMessage(
                        routing_key, body, self.body_parser(decoded_body), method, props
                    )
                    call_middlewares(
                        message,
                        itertools.chain(list(self.middlewares), [user_consumer]),
                    )

                    if not auto_ack:
                        # ack message after fn was ran
                        channel.basic_ack(method.delivery_tag)
                except Exception as err:  # pylint: disable=broad-except
                    self.app.logger.error(f"ERROR IN {queue_name}: {err}")
                    self.app.logger.exception(err)

                    try:
                        if not auto_ack:
                            channel.basic_reject(
                                method.delivery_tag, requeue=(not method.redelivered)
                            )
                    finally:
                        if self.on_message_error_callback is not None:
                            self.on_message_error_callback(
                                queue_name,
                                dead_letter_queue_name,
                                method,
                                props,
                                decoded_body,
                                err,
                            )

        channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=auto_ack
        )

        try:
            channel.start_consuming()
        except Exception as err:
            self.app.logger.error(err)
            channel.stop_consuming()
            connection.close()

            raise AMQPConnectionError from err

    def send(
        self,
        body,
        routing_key: str,
        exchange_type: ExchangeType = ExchangeType.DEFAULT,
        retries: int = 5,
        message_version: str = "v1.0.0",
        **properties,
    ):
        """Sends a message to a given routing key

        Args:
            body (str): The body to be sent
            routing_key (str): The routing key for the message
            exchange_type (ExchangeType, optional): The exchange type to be used. Defaults to ExchangeType.DEFAULT.
            retries (int, optional): Number of retries to send the message. Defaults to 5.
            message_version (str): Message version number.
            properties (dict[str, Any]): Additional properties to pass to spec.BasicProperties
        """

        thread = Thread(
            target=lambda: self.sync_send(
                body, routing_key, exchange_type, retries, message_version, **properties
            ),
        )
        thread.daemon = True
        thread.start()

    def sync_send(
        self,
        body,
        routing_key: str,
        exchange_type: ExchangeType = ExchangeType.DEFAULT,
        retries: int = 5,
        message_version: str = "v1.0.0",
        **properties,
    ):
        """Sends a message to a given routing key synchronously

        Args:
            body (str): The body to be sent
            routing_key (str): The routing key for the message
            exchange_type (ExchangeType, optional): The exchange type to be used. Defaults to ExchangeType.DEFAULT.
            retries (int, optional): Number of retries to send the message. Defaults to 5.
            message_version (str): Message version number.
            properties (dict[str, Any]): Additional properties to pass to spec.BasicProperties
        """

        retry_call(
            self._send_msg,
            (body, routing_key, exchange_type, message_version),
            properties,
            exceptions=(AMQPConnectionError, AssertionError),
            tries=retries,
            delay=5,
            jitter=(5, 15),
        )

    def _send_msg(
        self,
        body,
        routing_key,
        exchange_type,
        message_version: str = "v1.0.0",
        **properties,
    ):
        try:
            channel = self.get_connection().channel()

            channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type=exchange_type,
                durable=self.durable_exchange,
                passive=self.passive_exchange,
            )

            if self.msg_parser:
                body = self.msg_parser(body)

            if "message_id" not in properties:
                properties["message_id"] = sha256(
                    json.dumps(body).encode("utf-8")
                ).hexdigest()
            if "timestamp" not in properties:
                properties["timestamp"] = int(datetime.now().timestamp())

            if "headers" not in properties:
                properties["headers"] = {}
            properties["headers"]["x-message-version"] = message_version

            channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=body,
                properties=spec.BasicProperties(**properties),
            )

            channel.close()
        except Exception as err:
            self.app.logger.error("Error while sending message")
            self.app.logger.error(err)

            raise AMQPConnectionError from err
