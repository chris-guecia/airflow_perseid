import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


def send_slack_notification(
    message: str,
    status: str,
    context=None,
    token: str = os.environ.get('SLACK_API_TOKEN'),
    channel: str = '#alerts-prescient'
):
    """Sends a Slack message with the given status and context."""

    emojis = {
        "success": ":white_check_mark:",
        "failure": ":red_circle:",
        "retrying": ":arrows_counterclockwise:",
    }

    # Add status-specific information
    if status == "failure" and context:
        message += f"""
                    *Dag*: {context.get('task_instance').dag_id}
                    *Task*: {context.get('task_instance').task_id}
                    *Execution Time*: {context.get('execution_date')}
                    *Log Url*: {context.get('task_instance').log_url}
                    """
    elif status == "success":
        message += f"""
                    *Dag*: {context['dag'].dag_id}
                    *Task*: {context.get('task_instance').task_id}
                    *Execution Time*: {context['ts']}
                    """
    elif status == "retrying":
        task_instance = context.get('task_instance')
        if task_instance:
            message += f"""
                    *Dag*: {task_instance.dag_id}
                    *Task*: {task_instance.task_id}
                    *Retry Number*: {task_instance.try_number}
                    """

    # Construct the final message
    full_message = f"{emojis[status]} {message}"

    # Send the message
    try:
        client = WebClient(token=token)
        client.chat_postMessage(channel=channel, text=full_message)
    except SlackApiError as e:
        print(f"Slack notification failed: {e}")


def on_retry_callback(context):
    send_slack_notification("Task up for retry", "retrying", context=context)


def on_failure_callback(context):
    send_slack_notification("Task failed", "failure", context=context)


def on_success_callback(context):
    send_slack_notification("Task succeeded", "success", context=context)
