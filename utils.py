from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os
from dotenv import load_dotenv

load_dotenv()
OPENWEATHER_API_KEY = os.environ.get('OPENWEATHER_API_KEY')


def send_slack_message(message: str, token: str, channel: str):
    try:
        client = WebClient(token=token)
        response = client.chat_postMessage(channel=channel, text=message)
        print(response)
        return response
    except SlackApiError as e:
        print(f"This failed--> {e}")


send_slack_message("New alert! Hello Slack Channel!", OPENWEATHER_API_KEY, "#alerts-prescient")
