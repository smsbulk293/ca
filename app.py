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

# ensure simple data files exist
def ensure_file(path, default):
    if not os.path.exists(path):
        with open(path, 'w', encoding='utf8') as f:
            json.dump(default, f, indent=2)

ensure_file(WALLET_PATH, {"balance": 100000})
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

def normalize_phone(raw: str, default_region: str = 'IN'):
    if not raw:
        return None
    raw = raw.strip()
    try:
        pn = phonenumbers.parse(raw, default_region)
        if phonenumbers.is_valid_number(pn):
            return phonenumbers.format_number(pn, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        pass
    # fallback: strip non-digits and add leading + if present
    digits = ''.join(ch for ch in raw if ch.isdigit())
    if raw.startswith('+') and digits:
        return '+' + digits
    # no plus sign — return digits (may be incomplete)
    if len(digits) >= 8:
        return digits
    return None

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
      "pricePerSegment": 50,
      "defaultCountry": "IN",
      "send": false|true
    }
    """
    payload = request.get_json(force=True)
    csv_text = payload.get('csv') or ''
    template = payload.get('template') or ''
    price_per_segment = int(payload.get('pricePerSegment') or 0)
    default_country = payload.get('defaultCountry') or 'IN'
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
    for r in rows:
        raw_phone = (r.get('phone') or r.get('phone_number') or r.get('mobile') or r.get('msisdn') or '').strip()
        if not raw_phone:
            continue
        phone = normalize_phone(raw_phone, default_country)
        if not phone:
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

    total_cost = total_segments * price_per_segment

    if not do_send:
        return jsonify(rows=parsed, totalSegments=total_segments, totalCost=total_cost)

    # send == true -> reserve wallet, create job & recipients
    with fs_lock:
        wallet = read_json(WALLET_PATH)
        if wallet.get('balance', 0) < total_cost:
            return jsonify(error='Insufficient wallet balance'), 402
        wallet['balance'] = wallet.get('balance', 0) - total_cost
        write_json_atomic(WALLET_PATH, wallet)

        jobs = read_json(JOBS_PATH)
        job_id = str(uuid.uuid4())
        job = {
            "id": job_id,
            "totalRecipients": len(parsed),
            "totalSegments": total_segments,
            "totalCost": total_cost,
            "pricePerSegment": price_per_segment,
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

    return jsonify(ok=True, jobId=job_id, rows=parsed, totalSegments=total_segments, totalCost=total_cost)

@app.route('/api/wallet', methods=['GET'])
def api_wallet():
    wallet = read_json(WALLET_PATH)
    return jsonify(wallet)

# admin topup: protected by ADMIN_TOKEN header
@app.route('/api/admin/topup', methods=['POST'])
def api_topup():
    token = request.headers.get('X-ADMIN-TOKEN', '')
    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        return jsonify(error='unauthorized'), 401
    body = request.get_json(force=True)
    amount = int(body.get('amount') or 0)
    if amount <= 0:
        return jsonify(error='amount required'), 400
    with fs_lock:
        wallet = read_json(WALLET_PATH)
        wallet['balance'] = wallet.get('balance', 0) + amount
        write_json_atomic(WALLET_PATH, wallet)
    return jsonify(ok=True, balance=wallet['balance'])

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

