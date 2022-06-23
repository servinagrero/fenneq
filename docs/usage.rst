Usage guide
===========

Creating an agent
~~~~~~~~~~~~~~~~~

In order to create an agent the following parameters need to be provided.

.. list-table::
    :header-rows: 0

    * - url
      - String formatted with user and password
    * - node
      - RabbitMQ topic to bind the agent
    * - exchange
      - RabbitMQ exchange to listen to messages
    * - msg_type
      - Whether the message should be parsed to JSON or bytes.


.. code-block:: python
    :caption: Creation of an agent
    
    from fenneq import Agent

    url = "amqp://user:pass@localhost"
    node = "agent.test"
    exchange = "Name of RabbitMQ exchange"
    msg_type = Agent.JSON
    agent = Agent(url, exchange, node)


Registering a callback
~~~~~~~~~~~~~~~~~~~~~~

In order to add functionality to an agent, callbacks need to be registered on a handler. A callback is registered by using the :meth:`~fenneq.Agent.on` method.


.. code-block:: python
    
    @agent.on(True) # Run on any message
    @agent.on("baz") # Run if string `baz` inside the message
    @agent.on({"foo": "bar"}) # Run if key `foo` has value `bar`
    @agent.on({"foo": str}) # Run if key foo `present` and is string
    def hello()
        return "hello world"


Multiple agents can register the same callback for a same handler. The same agent can register a callback multiple times under different handlers.

.. code-block:: python
    
    @agent.on({"foo": "bar"})
    @agent.on("baz")
    @agent_2.on("foo")
    def hello()
        return "hello world"


Running an agent
~~~~~~~~~~~~~~~~

Tu run an agent, the method :meth:`~fenneq.Agent.run` has to be called. If there are no handlers registered, the agent returns a `RuntimeError`. **The agent runs in a blocking loop**.

.. code-block:: python

    @agent.on("bar")
    def hello():
        print("hello world")
        
    if __name__ == '__main__':
        agent.run()


Sending a message
~~~~~~~~~~~~~~~~~

A message can be sent easily to a running agent by using the same configuring another agent. If the agent is configured to use JSON, the message is serialized before sending.


.. code-block:: python

    from fenneq import Agent
    agent = Agent(...)
    agent.run()

    # ...
    # In another file
    agent = Agent(...)
    msg = {"msg": "Hello world!"}
    agent.send(msg)
