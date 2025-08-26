# poller_lambda.py
import os
import json
import logging
import requests
import boto3
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SERVICENOW_BASE = os.environ['SERVICENOW_BASE_URL'].rstrip('/')
SN_USER = os.environ['SERVICENOW_USER']
SN_PASS = os.environ['SERVICENOW_PASS']
ASSIGNMENT_GROUPS = os.environ.get('ASSIGNMENT_GROUPS', '')  # comma-separated sys_ids
SQS_URL = os.environ['SQS_QUEUE_URL']
POLL_LOOKBACK_MIN = int(os.environ.get('POLL_LOOKBACK_MINUTES', '10'))

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))

def fetch_incidents(group_sys_ids, since_dt):
    # Build ServiceNow query:
    # - incidents in groups
    # - assigned_toISEMPTY (unassigned)
    # - sys_created_on>since_dt
    group_list = ','.join(group_sys_ids)
    since_str = since_dt.strftime('%Y-%m-%d %H:%M:%S')
    params = {
        'sysparm_fields': 'sys_id,number,assignment_group,assignment_group.name,priority,opened_at,sys_created_on,assigned_to',
        'sysparm_query': f'assignment_groupIN{group_list}^assigned_toISEMPTY^active=true^sys_created_on>javascript:gs.dateGenerate("{since_str}","00:00:00")',
        'sysparm_limit': '200'
    }
    url = f"{SERVICENOW_BASE}/api/now/table/incident"
    logger.info("Querying ServiceNow: %s", params['sysparm_query'])
    resp = requests.get(url, auth=(SN_USER, SN_PASS), params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get('result', [])

def push_to_sqs(item):
    body = json.dumps(item)
    resp = sqs.send_message(QueueUrl=SQS_URL, MessageBody=body)
    logger.info("Pushed incident %s to SQS messageId=%s", item.get('number'), resp.get('MessageId'))

def lambda_handler(event, context):
    try:
        groups = [g.strip() for g in ASSIGNMENT_GROUPS.split(',') if g.strip()]
        if not groups:
            raise ValueError("No assignment groups configured.")
        since = datetime.utcnow() - timedelta(minutes=POLL_LOOKBACK_MIN)
        incidents = fetch_incidents(groups, since)
        logger.info("Fetched %d incidents", len(incidents))
        for inc in incidents:
            # sanitize and push required fields
            msg = {
                'sys_id': inc.get('sys_id'),
                'number': inc.get('number'),
                'assignment_group_sys_id': inc.get('assignment_group'),
                'assignment_group_name': inc.get('assignment_group', {}).get('value') if isinstance(inc.get('assignment_group'), dict) else inc.get('assignment_group'),
                'priority': inc.get('priority'),
                'opened_at': inc.get('opened_at') or inc.get('sys_created_on'),
                'fetched_at': datetime.utcnow().isoformat()
            }
            push_to_sqs(msg)
    except Exception as e:
        logger.exception("Poller error: %s", e)
        raise