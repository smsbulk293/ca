# app.py
import os
import csv
import json
import uuid
from datetime import datetime
from flask import Flask, request, jsonify, render_template, send_from_directory, abort
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

# ensure simple data files exist (wallet uses mills: 1 mill = $0.001)
def ensure_file(path, default):
    if not os.path.exists(path):
        with open(path, 'w', encoding='utf8') as f:
            json.dump(default, f, indent=2)

# Example: balance_mills: 100000 => $100.000
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

# Admin token for simple protection
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', '')

# --- constants for your new requirements ---
# Price per SMS = $0.023 => 23 mills (1 mill = $0.001)
PRICE_PER_SMS_MILLS = 23
# Only allow Canada numbers
ALLOWED_REGION = 'CA'

# --- helpers: segments & phone normalization ---
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
    """
    Returns E.164 phone if valid and in Canada; otherwise returns (None, error_message)
    """
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
        # fallback: simple check for +1 ... but we require proper Canada validation, so reject
        return None, 'could not parse phone number'

def mills_to_usd_string(mills: int) -> str:
    # mills -> dollars with 3 decimal places
    dollars = mills / 1000.0
    return f"${dollars:,.3f}"

# --- routes ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/estimate', methods=['POST'])
def api_estimate():
    """
    POST JSON:
    {
      "csv": "<csv text>",
      "template": "<template with {{name}}>",
      "defaultCountry": "CA",
      "send": false|true
    }
    Note: price per sms is fixed server-side to $0.023 (23 mills).
    """
    payload = request.get_json(force=True)
    csv_text = payload.get('csv') or ''
    template = payload.get('template') or ''
    default_country = payload.get('defaultCountry') or 'CA'
    do_send = payload.get('send') is True

    if not csv_text:
        return jsonify(error='CSV missing'), 400

    # parse CSV robustly
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
        return jsonify(rows=parsed, totalSegments=total_segments, totalCost_mills=total_cost_mills,
                       totalCost_usd=mills_to_usd_string(total_cost_mills), rejected=rejected)

    # send == true -> reserve wallet, create job & recipients
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

    return jsonify(ok=True, jobId=job_id, rows=parsed, totalSegments=total_segments,
                   totalCost_mills=total_cost_mills, totalCost_usd=mills_to_usd_string(total_cost_mills),
                   rejected=rejected)

@app.route('/api/wallet', methods=['GET'])
def api_wallet():
    wallet = read_json(WALLET_PATH)
    balance_mills = wallet.get('balance_mills', 0)
    return jsonify(balance_mills=balance_mills, balance_usd=mills_to_usd_string(balance_mills))

# admin topup: protected by ADMIN_TOKEN header (amount in mills)
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

# Twilio delivery webhook (Twilio POSTS form data)
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

# job status endpoint
@app.route('/api/job/<job_id>', methods=['GET'])
def api_job(job_id):
    jobs = read_json(JOBS_PATH)
    job = next((j for j in jobs if j.get('id') == job_id), None)
    if not job:
        return jsonify(error='job not found'), 404
    recips = read_json(RECIPS_PATH)
    job_recs = [r for r in recips if r.get('jobId') == job_id]
    return jsonify(job=job, recipients=job_recs)

# simple static files if needed
@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

if __name__ == '__main__':
    port = int(os.getenv('PORT', '5000'))
    # debug True is convenient for dev; disable for production
    app.run(host='0.0.0.0', port=port, debug=True)
