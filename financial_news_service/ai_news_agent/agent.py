import ollama
import json
import logging
from typing import List, Dict, Any

from . import tools

class NewsAgent:
    """
    An AI agent that can reason, plan, and use tools to find relevant
    financial news for a given company.
    """
    def __init__(self, model="llama3:70b"):
        self.model = model
        self.history = []

    def _get_available_tools(self):
        return {
            "web_search": tools.web_search,
            "browse_website": tools.browse_website
        }

    def run(self, company_name: str, ticker_symbol: str) -> List[Dict[str, Any]]:
        """
        Runs the agent to achieve the goal of finding news for a company.
        """
        goal = f"Find the top 3-5 most recent and relevant news articles specifically about the corporate entity '{company_name}' (ticker: {ticker_symbol}). For each article, provide its title, URL, and the full text content."
        self.history = [{"role": "user", "content": f"Here is your goal: {goal}"}]
        
        final_articles = []
        max_iterations = 5 # To prevent infinite loops

        for i in range(max_iterations):
            logging.info(f"--- Agent Iteration {i+1} ---")
            
            prompt = self._construct_prompt()
            response = ollama.chat(
                model=self.model,
                messages=[{"role": "system", "content": prompt}, *self.history],
                format="json"
            )
            
            try:
                decision = json.loads(response['message']['content'])
                logging.info(f"Agent Decision: {decision}")
                
                self.history.append({"role": "assistant", "content": json.dumps(decision)})

                if decision.get("tool") == "finish":
                    logging.info("Agent has decided to finish.")
                    final_articles = decision.get("result", [])
                    break
                
                tool_name = decision.get("tool")
                tool_args = decision.get("args", {})
                
                available_tools = self._get_available_tools()
                if tool_name in available_tools:
                    tool_function = available_tools[tool_name]
                    result = tool_function(**tool_args)
                    self.history.append({"role": "tool", "content": json.dumps(result)})
                else:
                    self.history.append({"role": "tool", "content": f"Error: Tool '{tool_name}' not found."})

            except json.JSONDecodeError as e:
                logging.error(f"Agent failed to produce valid JSON: {e}")
                self.history.append({"role": "tool", "content": "Error: Your last response was not valid JSON. Please try again."})
                continue
        
        return final_articles

    def _construct_prompt(self):
        return """
        You are an autonomous financial news agent. Your goal is to find relevant news articles.
        You have access to the following tools:
        1. `web_search(query: str, num_results: int)`: Performs a web search and returns a list of URLs and titles.
        2. `browse_website(url: str)`: Fetches the full text content of a given URL.

        Based on the conversation history, you must decide on your next action.
        Your response MUST be a single JSON object with one of two formats:

        Format 1: To use a tool
        {
          "thought": "Your reasoning for choosing this tool and these arguments.",
          "tool": "tool_name_to_use",
          "args": {"arg_name": "value", ...}
        }

        Format 2: When you have successfully gathered enough high-quality articles to satisfy the user's goal
        {
          "thought": "I have found enough relevant articles and will now finish.",
          "tool": "finish",
          "result": [
            {"title": "Article 1 Title", "url": "http://...", "content": "Full text of article 1..."},
            {"title": "Article 2 Title", "url": "http://...", "content": "Full text of article 2..."}
          ]
        }
        
        Begin.
        """
