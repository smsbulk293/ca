# app.py — background-thread sender (free-plan friendly)
import os
import csv
import json
import uuid
import time
import threading
from datetime import datetime
from flask import Flask, request, jsonify, render_template, send_from_directory
from twilio.rest import Client
import phonenumbers
from threading import Lock

# --- App setup ---
app = Flask(__name__, template_folder='templates', static_folder='static')
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

WALLET_PATH = os.path.join(DATA_DIR, 'wallet.json')
JOBS_PATH = os.path.join(DATA_DIR, 'jobs.json')
RECIPS_PATH = os.path.join(DATA_DIR, 'recipients.json')

def ensure_file(path, default):
    if not os.path.exists(path):
        with open(path, 'w', encoding='utf8') as f:
            json.dump(default, f, indent=2)

# initialize data files (wallet uses mills: 1 mill = $0.001)
ensure_file(WALLET_PATH, {"balance_mills": 100000})
ensure_file(JOBS_PATH, [])
ensure_file(RECIPS_PATH, [])

fs_lock = Lock()

def read_json(path):
    with open(path, 'r', encoding='utf8') as f:
        return json.load(f)

def write_json_atomic(path, data):
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf8') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)

# Twilio client (if env set)
TW_SID = os.getenv('TWILIO_ACCOUNT_SID', '')
TW_TOKEN = os.getenv('TWILIO_AUTH_TOKEN', '')
TW_FROM = os.getenv('TWILIO_FROM', '')
PUBLIC_WEBHOOK = os.getenv('PUBLIC_WEBHOOK_URL', '')
tw_client = None
if TW_SID and TW_TOKEN:
    tw_client = Client(TW_SID, TW_TOKEN)

ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', '')

# constants
PRICE_PER_SMS_MILLS = 23   # $0.023
ALLOWED_REGION = 'CA'
SEND_DELAY_MS = int(os.getenv('SEND_DELAY_MS', '250'))
MAX_IMMEDIATE_RETRIES = 3  # per-recipient immediate retry attempts for transient errors

# helpers
def is_gsm7(text: str) -> bool:
    for ch in text or '':
        if ord(ch) > 127:
            return False
    return True

def segments_for_text(text: str) -> int:
    txt = text or ''
    l = len(txt)
    if is_gsm7(txt):
        if l <= 160:
            return 1
        return (l + 152) // 153
    else:
        if l <= 70:
            return 1
        return (l + 66) // 67

def normalize_phone_and_check_canada(raw: str, default_region: str = 'CA'):
    if not raw:
        return None, 'empty phone'
    raw = raw.strip()
    try:
        pn = phonenumbers.parse(raw, default_region)
        if not phonenumbers.is_valid_number(pn):
            return None, 'invalid phone number'
        region = phonenumbers.region_code_for_number(pn)
        if region != ALLOWED_REGION:
            return None, f'not a Canadian number (region={region})'
        return phonenumbers.format_number(pn, phonenumbers.PhoneNumberFormat.E164), None
    except Exception:
        return None, 'could not parse phone number'

def mills_to_usd_string(mills: int) -> str:
    dollars = mills / 1000.0
    return f"${dollars:,.3f}"

# -----------------------
# Background job processor
# -----------------------
def process_job(job_id):
    """
    Runs in a background thread.
    Picks queued recipients for the job and sends them sequentially (throttled).
    Updates recipient statuses and job record, and refunds unused mills at end.
    """
    print(f"[worker-thread] Starting job {job_id}")
    sent_segments = 0
    failed_segments = 0

    while True:
        # load recipients for this job with status queued or sending (resume semantics)
        with fs_lock:
            recips = read_json(RECIPS_PATH)
            job_recs = [r for r in recips if r.get('jobId') == job_id and r.get('status') in ('queued', 'sending')]
        if not job_recs:
            break  # nothing left to send

        # pick one recipient (FIFO)
        rec = job_recs[0]
        rec_id = rec['id']
        phone = rec['phone']
        msg_text = rec['message']
        segs = rec.get('segments', 1)

        # mark as sending and increment attempts
        with fs_lock:
            recips = read_json(RECIPS_PATH)
            target = next((x for x in recips if x['id'] == rec_id), None)
            if not target:
                # already processed
                continue
            target['status'] = 'sending'
            target['attempts'] = target.get('attempts', 0) + 1
            target['lastAttemptAt'] = datetime.utcnow().isoformat() + 'Z'
            write_json_atomic(RECIPS_PATH, recips)

        # try send (with a few immediate retries for transient errors)
        success = False
        last_err = None
        for attempt in range(MAX_IMMEDIATE_RETRIES):
            try:
                if not tw_client:
                    raise RuntimeError('Twilio not configured')
                msg = tw_client.messages.create(
                    body=msg_text,
                    to=phone,
                    from_=TW_FROM,
                    status_callback=(PUBLIC_WEBHOOK.rstrip('/') + '/api/twilio/status') if PUBLIC_WEBHOOK else None
                )
                # success
                success = True
                tw_sid = getattr(msg, 'sid', None)
                with fs_lock:
                    recips = read_json(RECIPS_PATH)
                    target = next((x for x in recips if x['id'] == rec_id), None)
                    if target:
                        target['twilioSid'] = tw_sid
                        target['status'] = 'sent'
                        target['lastSend'] = datetime.utcnow().isoformat() + 'Z'
                        write_json_atomic(RECIPS_PATH, recips)
                sent_segments += segs
                break
            except Exception as e:
                last_err = str(e)
                # transient wait small backoff
                time.sleep(0.5 + attempt * 0.5)
                continue

        if not success:
            # mark failed (no more immediate retries)
            with fs_lock:
                recips = read_json(RECIPS_PATH)
                target = next((x for x in recips if x['id'] == rec_id), None)
                if target:
                    target['lastError'] = last_err
                    target['status'] = 'failed'
                    write_json_atomic(RECIPS_PATH, recips)
            failed_segments += segs

        # throttle between sends
        time.sleep(SEND_DELAY_MS / 1000.0)

    # job finished: compute refund and update job record
    with fs_lock:
        jobs = read_json(JOBS_PATH)
        job = next((j for j in jobs if j.get('id') == job_id), None)
        if job:
            reserved_mills = job.get('totalCost_mills', 0)
            actual_cost_mills = sent_segments * PRICE_PER_SMS_MILLS
            refund_mills = max(0, reserved_mills - actual_cost_mills)
            # refund
            if refund_mills > 0:
                wallet = read_json(WALLET_PATH)
                wallet['balance_mills'] = wallet.get('balance_mills', 0) + refund_mills
                write_json_atomic(WALLET_PATH, wallet)
            job['status'] = 'completed'
            job['sent_segments'] = sent_segments
            job['failed_segments'] = failed_segments
            job['actual_cost_mills'] = actual_cost_mills
            job['refund_mills'] = refund_mills
            job['completedAt'] = datetime.utcnow().isoformat() + 'Z'
            write_json_atomic(JOBS_PATH, jobs)
    print(f"[worker-thread] Completed job {job_id}: sent={sent_segments} failed={failed_segments}")

def start_background_worker_for(job_id):
    t = threading.Thread(target=process_job, args=(job_id,), daemon=True)
    t.start()
    return t

# On startup: resume any queued recipients (safety/resume)
def resume_pending_jobs_on_startup():
    with fs_lock:
        recips = read_json(RECIPS_PATH)
        pending_job_ids = sorted({r['jobId'] for r in recips if r.get('status') in ('queued', 'sending')})
    for jid in pending_job_ids:
        print(f"[startup] Resuming pending job {jid}")
        start_background_worker_for(jid)

# schedule resume once when app starts
resume_pending_jobs_on_startup()

# -----------------------
# HTTP endpoints
# -----------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/estimate', methods=['POST'])
def api_estimate():
    payload = request.get_json(force=True)
    csv_text = payload.get('csv') or ''
    template = payload.get('template') or ''
    default_country = payload.get('defaultCountry') or 'CA'
    do_send = payload.get('send') is True

    if not csv_text:
        return jsonify(error='CSV missing'), 400

    try:
        reader = csv.DictReader(csv_text.splitlines())
        rows = [r for r in reader if any((v or '').strip() for v in r.values())]
    except Exception as e:
        return jsonify(error='CSV parse error: ' + str(e)), 400

    parsed = []
    total_segments = 0
    rejected = []
    for r in rows:
        raw_phone = (r.get('phone') or r.get('phone_number') or r.get('mobile') or r.get('msisdn') or '').strip()
        if not raw_phone:
            rejected.append({'row': r, 'reason': 'phone missing'})
            continue
        phone, err = normalize_phone_and_check_canada(raw_phone, default_country)
        if err:
            rejected.append({'row': r, 'reason': err})
            continue
        if (r.get('message') or '').strip():
            message = r.get('message').strip()
        else:
            message = template
            for k, v in r.items():
                message = message.replace('{{' + k + '}}', v or '')
        seg = segments_for_text(message)
        total_segments += seg
        parsed.append({'phone': phone, 'message': message, 'segments': seg, 'original': r})

    total_cost_mills = total_segments * PRICE_PER_SMS_MILLS

    if not do_send:
        return jsonify(rows=parsed, totalSegments=total_segments,
                       totalCost_mills=total_cost_mills, totalCost_usd=mills_to_usd_string(total_cost_mills),
                       rejected=rejected)

    # reserve wallet and create job & recipients, then start background processing
    with fs_lock:
        wallet = read_json(WALLET_PATH)
        if wallet.get('balance_mills', 0) < total_cost_mills:
            return jsonify(error='Insufficient wallet balance', required_mills=total_cost_mills,
                           required_usd=mills_to_usd_string(total_cost_mills)), 402
        wallet['balance_mills'] = wallet.get('balance_mills', 0) - total_cost_mills
        write_json_atomic(WALLET_PATH, wallet)

        jobs = read_json(JOBS_PATH)
        job_id = str(uuid.uuid4())
        job = {
            "id": job_id,
            "totalRecipients": len(parsed),
            "totalSegments": total_segments,
            "totalCost_mills": total_cost_mills,
            "totalCost_usd": mills_to_usd_string(total_cost_mills),
            "pricePerSegment_mills": PRICE_PER_SMS_MILLS,
            "pricePerSegment_usd": mills_to_usd_string(PRICE_PER_SMS_MILLS),
            "status": "queued",
            "createdAt": datetime.utcnow().isoformat() + 'Z'
        }
        jobs.append(job)
        write_json_atomic(JOBS_PATH, jobs)

        recips = read_json(RECIPS_PATH)
        now = datetime.utcnow().isoformat() + 'Z'
        for p in parsed:
            recips.append({
                "id": str(uuid.uuid4()),
                "jobId": job_id,
                "phone": p['phone'],
                "message": p['message'],
                "segments": p['segments'],
                "status": "queued",
                "attempts": 0,
                "createdAt": now
            })
        write_json_atomic(RECIPS_PATH, recips)

    # start background worker thread and return immediately
    start_background_worker_for(job_id)

    return jsonify(ok=True, jobId=job_id, totalCost_mills=total_cost_mills,
                   totalCost_usd=mills_to_usd_string(total_cost_mills), rejected=rejected)

@app.route('/api/wallet', methods=['GET'])
def api_wallet():
    wallet = read_json(WALLET_PATH)
    balance_mills = wallet.get('balance_mills', 0)
    return jsonify(balance_mills=balance_mills, balance_usd=mills_to_usd_string(balance_mills))

@app.route('/api/admin/topup', methods=['POST'])
def api_topup():
    token = request.headers.get('X-ADMIN-TOKEN', '')
    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        return jsonify(error='unauthorized'), 401
    body = request.get_json(force=True)
    amount_mills = int(body.get('amount_mills') or 0)
    if amount_mills <= 0:
        return jsonify(error='amount_mills required (integer)'), 400
    with fs_lock:
        wallet = read_json(WALLET_PATH)
        wallet['balance_mills'] = wallet.get('balance_mills', 0) + amount_mills
        write_json_atomic(WALLET_PATH, wallet)
    return jsonify(ok=True, balance_mills=wallet['balance_mills'], balance_usd=mills_to_usd_string(wallet['balance_mills']))

@app.route('/api/twilio/status', methods=['POST'])
def api_twilio_status():
    sid = request.form.get('MessageSid') or request.form.get('SmsSid')
    status = request.form.get('MessageStatus') or request.form.get('SmsStatus')
    to = request.form.get('To')

    if not sid and not to:
        return '', 200

    with fs_lock:
        recips = read_json(RECIPS_PATH)
        found = None
        if sid:
            for r in recips:
                if r.get('twilioSid') == sid:
                    found = r
                    break
        if not found and to:
            for r in recips:
                if r.get('phone') == to and r.get('status') in ('queued', 'sending', 'sent'):
                    found = r
                    break
        if found:
            found['twilioStatus'] = status
            if status == 'delivered':
                found['status'] = 'delivered'
            elif status in ('failed', 'undelivered'):
                found['status'] = 'failed'
            elif status == 'sent':
                found['status'] = 'sent'
            found['updatedAt'] = datetime.utcnow().isoformat() + 'Z'
            write_json_atomic(RECIPS_PATH, recips)
    return '', 200

@app.route('/api/job/<job_id>', methods=['GET'])
def api_job(job_id):
    jobs = read_json(JOBS_PATH)
    job = next((j for j in jobs if j.get('id') == job_id), None)
    if not job:
        return jsonify(error='job not found'), 404
    recips = read_json(RECIPS_PATH)
    job_recs = [r for r in recips if r.get('jobId') == job_id]
    return jsonify(job=job, recipients=job_recs)

@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

if __name__ == '__main__':
    port = int(os.getenv('PORT', '5000'))
    app.run(host='0.0.0.0', port=port, debug=True)
