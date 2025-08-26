# processor_lambda.py
import os
import json
import logging
import requests
import pymysql
import boto3
from datetime import datetime, timezone
import random
import math

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ServiceNow
SERVICENOW_BASE = os.environ['SERVICENOW_BASE_URL'].rstrip('/')
SN_USER = os.environ['SERVICENOW_USER']
SN_PASS = os.environ['SERVICENOW_PASS']

# DB (Aurora MySQL)
DB_HOST = os.environ['DB_HOST']
DB_PORT = int(os.environ.get('DB_PORT', 3306))
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_NAME = os.environ['DB_NAME']
MEMBER_TABLE = os.environ.get('MEMBER_DATA_TABLE', 'MEMBER_DATA')
HISTORY_TABLE = os.environ.get('ASSIGNMENT_HISTORY_TABLE', 'ASSIGNMENT_HISTORY')

# tuning
MAX_CANDIDATES = int(os.environ.get('MAX_CANDIDATES', '50'))

# other
SQS = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))

# Priority mapping and role multipliers (tune to your org)
PRIORITY_WEIGHT = {'1': 6.0, '2': 3.0, '3': 1.0, '4': 0.5, '5': 0.5}
SEVERITY_MULT = {'1': 1.3, '2': 1.1, '3': 1.0}
ROLE_MULT = {'L1': 1.20, 'L2': 1.00, 'L3': 0.90, 'SME': 0.85}

def get_db_conn():
    return pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS,
                           db=DB_NAME, port=DB_PORT, cursorclass=pymysql.cursors.DictCursor,
                           autocommit=True)

def fetch_group_members_from_servicenow(group_sys_id):
    # Query sys_user_grmember table to get user sys_ids
    url = f"{SERVICENOW_BASE}/api/now/table/sys_user_grmember"
    params = {
        'sysparm_query': f'group={group_sys_id}^active=true',
        'sysparm_fields': 'user,role'
    }
    r = requests.get(url, auth=(SN_USER, SN_PASS), params=params, timeout=30)
    r.raise_for_status()
    rows = r.json().get('result', [])
    members = []
    for r0 in rows:
        user = r0.get('user')
        if isinstance(user, dict):
            members.append(user.get('value'))
        else:
            members.append(user)
    return list(set([m for m in members if m]))

def fetch_member_rows_from_db(member_ids, group_sys_id, incident_dt):
    # incident_dt is a datetime
    # Filter by active and shift days/time - simple local-time check (expects shift times stored in DB)
    if not member_ids:
        return []
    placeholders = ','.join(['%s'] * len(member_ids))
    sql = f"SELECT * FROM {MEMBER_TABLE} WHERE member_sys_id IN ({placeholders}) AND assignment_group_sys_id=%s AND active=1"
    params = member_ids + [group_sys_id]
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        conn.close()
    # Filter rows by shift membership based on incident_dt weekday and time
    filtered = []
    incident_local = incident_dt  # assume UTC or align timezone with stored times
    incident_time = incident_local.time()
    weekday = incident_local.strftime('%a')  # e.g. Mon/Tue
    for r in rows:
        # shift_days stored as comma-separated like 'Mon,Tue,...' or MySQL SET type; handle both
        shift_days = r.get('shift_days') or ''
        if weekday not in shift_days:
            # if weekend shift flag applicable
            if incident_local.weekday() >= 5 and not r.get('weekend_shift_flag'):
                continue
            if weekday not in shift_days:
                continue
        start = r.get('shift_start_time')
        end = r.get('shift_end_time')
        if start is None or end is None:
            continue
        # handle overnight shift where end < start
        if start <= incident_time <= end:
            filtered.append(r)
        else:
            # overnight
            if start > end:
                if incident_time >= start or incident_time <= end:
                    filtered.append(r)
    return filtered

def fetch_assigned_incidents_for_member(member_sys_id):
    # Query ServiceNow for incidents assigned to this user and active
    url = f"{SERVICENOW_BASE}/api/now/table/incident"
    q = f"assigned_to={member_sys_id}^active=true"
    params = {'sysparm_query': q, 'sysparm_fields': 'sys_id,number,priority,severity,opened_at', 'sysparm_limit': '200'}
    r = requests.get(url, auth=(SN_USER, SN_PASS), params=params, timeout=30)
    r.raise_for_status()
    return r.json().get('result', [])

def compute_base_workload(assigned_incidents, now_dt):
    base = 0.0
    for inc in assigned_incidents:
        p = inc.get('priority') or '3'
        s = inc.get('severity') or '3'
        opened_at = inc.get('opened_at') or inc.get('sys_created_on')
        try:
            opened_dt = datetime.fromisoformat(opened_at.replace('Z', '+00:00')).replace(tzinfo=None)
        except Exception:
            try:
                opened_dt = datetime.strptime(opened_at, '%Y-%m-%d %H:%M:%S')
            except Exception:
                opened_dt = now_dt
        age_hours = max(0.0, (now_dt - opened_dt).total_seconds() / 3600.0)
        age_factor = 1.0 + (age_hours / 24.0)
        p_weight = PRIORITY_WEIGHT.get(str(p), 1.0)
        s_mult = SEVERITY_MULT.get(str(s), 1.0)
        contrib = p_weight * s_mult * age_factor
        base += contrib
    return base

def select_best_member(candidates, incident_info):
    now_dt = datetime.utcnow()
    scores = []
    for c in candidates:
        member_id = c['member_sys_id']
        role = c.get('role') or 'L2'
        weight_mod = float(c.get('weight_modifier') or 1.0)
        # fetch assigned incidents for each member
        assigned = fetch_assigned_incidents_for_member(member_id)
        base_workload = compute_base_workload(assigned, now_dt)
        # fairness: count recent assignments (e.g., last 24h)
        recent_count = get_recent_assignment_count(member_id, hours=24)
        role_mult = ROLE_MULT.get(role, 1.0)
        final = (base_workload * role_mult * weight_mod) + 0.1 * recent_count
        final += random.uniform(0, 0.01)  # tiny jitter
        scores.append({
            'member_sys_id': member_id,
            'member_name': c.get('member_name'),
            'base_workload': base_workload,
            'role_mult': role_mult,
            'weight_mod': weight_mod,
            'recent_count': recent_count,
            'final_weight': final
        })
    if not scores:
        return None, []
    scores.sort(key=lambda x: x['final_weight'])
    return scores[0]['member_sys_id'], scores

def get_recent_assignment_count(member_sys_id, hours=24):
    # read ASSIGNMENT_HISTORY for the last N hours
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS cnt FROM {HISTORY_TABLE} WHERE assigned_to_member_sys_id=%s AND assignment_timestamp >= DATE_SUB(NOW(), INTERVAL %s HOUR)", (member_sys_id, hours))
            row = cur.fetchone()
            return int(row['cnt'] or 0)
    finally:
        conn.close()

def assign_incident_to_member(incident_sys_id, member_sys_id):
    url = f"{SERVICENOW_BASE}/api/now/table/incident/{incident_sys_id}"
    payload = {'assigned_to': member_sys_id}
    r = requests.patch(url, auth=(SN_USER, SN_PASS), json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def record_assignment_history(incident_sys_id, incident_number, member_sys_id, snapshot):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            sql = f"INSERT INTO {HISTORY_TABLE} (incident_sys_id, incident_number, assigned_to_member_sys_id, assignment_timestamp, algorithm_snapshot, success, created_by) VALUES (%s,%s,NOW(),%s,%s,%s,%s)"
            cur.execute(sql, (incident_sys_id, incident_number, member_sys_id, json.dumps(snapshot), True, 'lambda_processor'))
    finally:
        conn.close()

def confirm_incident_unassigned(incident_sys_id):
    url = f"{SERVICENOW_BASE}/api/now/table/incident/{incident_sys_id}"
    r = requests.get(url, auth=(SN_USER, SN_PASS), params={'sysparm_fields': 'assigned_to,assignment_group,sys_id,number'}, timeout=20)
    r.raise_for_status()
    return r.json().get('result', {})

def lambda_handler(event, context):
    # SQS event
    for rec in event.get('Records', []):
        body = json.loads(rec['body'])
        incident_sys_id = body.get('sys_id')
        incident_number = body.get('number')
        group_id = body.get('assignment_group_sys_id')
        opened_at = body.get('opened_at')
        try:
            # re-check incident state
            inc = confirm_incident_unassigned(incident_sys_id)
            if not inc:
                logger.warning("Incident %s not found, skipping", incident_sys_id)
                continue
            if inc.get('assigned_to'):
                logger.info("Incident %s already assigned, skipping", incident_number)
                continue
            # fetch group members from SN
            group_members = fetch_group_members_from_servicenow(group_id)
            logger.info("Group %s members fetched: %d", group_id, len(group_members))
            if not group_members:
                logger.warning("No members in group %s", group_id)
                continue
            # fetch MEMBER_DATA rows for these members and filter by shift
            incident_dt = datetime.fromisoformat(opened_at.replace('Z', '+00:00')).replace(tzinfo=None) if opened_at else datetime.utcnow()
            member_rows = fetch_member_rows_from_db(group_members, group_id, incident_dt)
            if not member_rows:
                logger.warning("No available members in MEMBER_DATA for group %s at time %s", group_id, incident_dt)
                continue
            # limit candidate count if needed
            candidates = member_rows[:MAX_CANDIDATES]
            selected_member, score_snapshot = select_best_member(candidates, body)
            if not selected_member:
                logger.warning("No candidate selected for incident %s", incident_number)
                continue
            # final confirm and assign
            # re-confirm assigned_to before attempt to minimize races
            inc_latest = confirm_incident_unassigned(incident_sys_id)
            if inc_latest.get('assigned_to'):
                logger.info("Incident %s was assigned during processing, skipping", incident_number)
                continue
            assign_resp = assign_incident_to_member(incident_sys_id, selected_member)
            # record history and snapshot
            algorithm_snapshot = {'scores': score_snapshot, 'chosen': selected_member}
            record_assignment_history(incident_sys_id, incident_number, selected_member, algorithm_snapshot)
            logger.info("Assigned incident %s to %s", incident_number, selected_member)
        except Exception as e:
            logger.exception("Processing incident %s failed: %s", incident_number, e)
            # Optionally requeue / dead-letter handle via SQS redrive
            continue