# worker.py
import os
import time
import json
from datetime import datetime
from twilio.rest import Client
from threading import Lock
import traceback

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')
RECIPS_PATH = os.path.join(DATA_DIR, 'recipients.json')
JOBS_PATH = os.path.join(DATA_DIR, 'jobs.json')
WALLET_PATH = os.path.join(DATA_DIR, 'wallet.json')

TW_SID = os.getenv('TWILIO_ACCOUNT_SID', '')
TW_TOKEN = os.getenv('TWILIO_AUTH_TOKEN', '')
TW_FROM = os.getenv('TWILIO_FROM', '')
PUBLIC_WEBHOOK = os.getenv('PUBLIC_WEBHOOK_URL', '')

client = None
if TW_SID and TW_TOKEN:
    client = Client(TW_SID, TW_TOKEN)

fs_lock = Lock()

def read_json(p):
    with open(p, 'r', encoding='utf8') as f:
        return json.load(f)

def write_json_atomic(p, data):
    tmp = p + '.tmp'
    with open(tmp, 'w', encoding='utf8') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, p)

def send_message(rec):
    if not client:
        raise RuntimeError('Twilio not configured (TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN missing)')
    to = rec['phone']
    body = rec['message']
    kwargs = {
        'body': body,
        'to': to
    }
    if TW_FROM:
        kwargs['from_'] = TW_FROM
    if PUBLIC_WEBHOOK:
        kwargs['status_callback'] = PUBLIC_WEBHOOK.rstrip('/') + '/api/twilio/status'
    msg = client.messages.create(**kwargs)
    return msg.sid

def worker_loop():
    CONCURRENCY = int(os.getenv('SEND_CONCURRENCY', '4'))
    DELAY_MS = int(os.getenv('SEND_DELAY_MS', '250'))
    print('Worker started: concurrency=', CONCURRENCY, 'delay_ms=', DELAY_MS)
    while True:
        try:
            with fs_lock:
                recips = read_json(RECIPS_PATH)
            queued = [r for r in recips if r.get('status') == 'queued' and r.get('attempts',0) < 4]
            if not queued:
                time.sleep(2)
                continue
            batch = queued[:CONCURRENCY]
            for r in batch:
                # mark attempt
                with fs_lock:
                    recips = read_json(RECIPS_PATH)
                    rr = next((x for x in recips if x['id'] == r['id']), None)
                    if not rr:
                        continue
                    rr['attempts'] = rr.get('attempts',0) + 1
                    rr['status'] = 'sending'
                    write_json_atomic(RECIPS_PATH, recips)
                try:
                    sid = send_message(rr)
                    with fs_lock:
                        recips = read_json(RECIPS_PATH)
                        rr = next((x for x in recips if x['id'] == r['id']), None)
                        if rr:
                            rr['twilioSid'] = sid
                            rr['status'] = 'sent'
                            rr['lastSend'] = datetime.utcnow().isoformat() + 'Z'
                            write_json_atomic(RECIPS_PATH, recips)
                except Exception as e:
                    print('Send error for', r.get('phone'), str(e))
                    traceback.print_exc()
                    with fs_lock:
                        recips = read_json(RECIPS_PATH)
                        rr = next((x for x in recips if x['id'] == r['id']), None)
                        if rr:
                            rr['lastError'] = str(e)
                            rr['status'] = 'failed' if rr.get('attempts',0) >= 4 else 'queued'
                            write_json_atomic(RECIPS_PATH, recips)
                time.sleep(DELAY_MS / 1000.0)
        except Exception as err:
            print('Worker loop error:', err)
            time.sleep(2)

if __name__ == '__main__':
    worker_loop()
