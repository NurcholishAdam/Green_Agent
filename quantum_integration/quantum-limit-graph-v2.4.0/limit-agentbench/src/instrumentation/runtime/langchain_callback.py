from langchain.callbacks.base import BaseCallbackHandler

class GreenLangChainCallback(BaseCallbackHandler):
    """
    Tracks token usage, tool calls, latency.
    """

    def __init__(self):
        self.token_usage = 0
        self.tool_calls = 0

    def on_llm_end(self, response, **kwargs):
        self.token_usage += response.llm_output["token_usage"]["total_tokens"]

    def on_tool_start(self, serialized, input_str, **kwargs):
        self.tool_calls += 1
