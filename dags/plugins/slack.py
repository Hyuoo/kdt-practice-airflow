from airflow.models import Variable

import logging
import requests

'''
https://hooks.slack.com/services/[slack_url]
'''

def on_failure_callback(context):
    """
    https://airflow.apache.org/_modules/airflow/operators/slack_operator.html
    Define the callback to post on Slack if a failure is detected in the Workflow
    :return: operator.execute
    """
    text = str(context['task_instance'])
    text += "\n```" + str(context.get('exception')) +"```"
    send_message_to_a_slack_channel(text, ":scream:")

'''
curl -X POST -H 'Content-type: application/json' --data '{"text":"Hello, World!"}'
https://hooks.slack.com/services/
T05DDLGM1A9/B05DK2Q0M52/hem7FvbGDgCC42HXiJSWU72X
'''

# def send_message_to_a_slack_channel(message, emoji, channel, access_token):
def send_message_to_a_slack_channel(message, emoji):
    # url = "https://slack.com/api/chat.postMessage"
    url = "https://hooks.slack.com/services/"+Variable.get("slack_url")
    headers = {
        'content-type': 'application/json',
    }
    data = { "username": "PY_SCRIPT", "text": message, "icon_emoji": emoji }
    r = requests.post(url, json=data, headers=headers)
    return r

if __name__=="__main__":
    on_failure_callback({"task_instance":"HYUOO","exception":"TEST"})
    on_failure_callback({"task_instance":"ASD","exception":"hello world"})