from litellm import ChatCompletionToolParam, ChatCompletionToolParamFunctionChunk

SendTask = ChatCompletionToolParam(
    type='function',
    function=ChatCompletionToolParamFunctionChunk(
        name='a2a_send_task',
        description="""
            Send a task to an A2A agent and yield task responses. Identify the agent that can help you with the task and send the task to it.
            Use this tool to send a task to a remote agent and yield task responses.
        """,
        parameters={
            'type': 'object',
            'properties': {
                'agent_name': {
                    'type': 'string',
                    'description': "The name of the A2A agent to send the task to. The agent's name should match strictly with the agent's card in the list of available agents in your system prompt.",
                },
                'task_message': {
                    'type': 'string',
                    'description': 'The message to send to the A2A agent.',
                },
            },
            'required': ['agent_name', 'task_message'],
        },
    ),
)
