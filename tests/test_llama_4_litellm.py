import os

from litellm import completion

# Define a tool (function) for the model to call
tools = [
    {
        'type': 'function',
        'function': {
            'name': 'get_weather',
            'description': 'Get the current weather for a city',
            'parameters': {
                'type': 'object',
                'properties': {
                    'city': {
                        'type': 'string',
                        'description': 'The city name, e.g., New York',
                    }
                },
                'required': ['city'],
            },
        },
    }
]


# Mock function to simulate weather data
def get_weather(city):
    return f'The weather in {city} is sunny with a temperature of 75°F.'


# Test 1: Basic tool calling
resp = completion(
    model='litellm_proxy/llama-4-maverick',
    api_key=os.getenv('LITELLM_API_KEY'),
    base_url='http://localhost:9545',
    messages=[{'role': 'user', 'content': 'What is the weather in New York?'}],
    tools=tools,
)

print('Test 1 - Basic tool calling:')
print(resp)
print('\n' + '=' * 80 + '\n')

# Test 2: Context limit test (~700k tokens)
# Approximate tokens: 1 token ≈ 4 characters for English text
# Target: 700k tokens ≈ 2.8M characters
# Using a repeated pattern to generate large context

base_text = 'The quick brown fox jumps over the lazy dog. This is a test sentence to fill the context window. '
# Each repetition is ~100 characters ≈ 25 tokens
# For 700k tokens: 700,000 / 25 = 28,000 repetitions
repetitions = 28000
large_context = base_text * repetitions

print('Test 2 - Context limit test:')
print(f'Generated text length: {len(large_context):,} characters')
print(f'Estimated tokens: ~{len(large_context) // 4:,}')

resp_large = completion(
    model='litellm_proxy/llama-4-maverick',
    api_key=os.getenv('LITELLM_API_KEY'),
    base_url='http://localhost:9545',
    messages=[
        {
            'role': 'user',
            'content': large_context
            + '\n\nPlease summarize the above text in one sentence.',
        }
    ],
)

print('\nResponse from large context test:')
print(resp_large)
