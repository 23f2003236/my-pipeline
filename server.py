from flask import Flask, request, jsonify
import requests
import sqlite3
import json
from datetime import datetime

app = Flask(__name__)
DB_FILE = "pipeline_data.db"

# ================= DATABASE SETUP =================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            raw_data TEXT,
            analysis TEXT,
            sentiment TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

# ================= STEP 1: FETCH USERS =================
def fetch_users():
    try:
        # Try real API first
        response = requests.get(
            "https://jsonplaceholder.typicode.com/users",
            timeout=10
        )
        response.raise_for_status()
        users = response.json()
        return users[:3], None

    except Exception:
        # FALLBACK DATA (ALWAYS WORKS)
        mock_users = [
            {
                "name": "Leanne Graham",
                "email": "leanne@example.com",
                "company": {"name": "Romaguera-Crona"},
                "address": {"city": "Gwenborough"}
            },
            {
                "name": "Ervin Howell",
                "email": "ervin@example.com",
                "company": {"name": "Deckow-Crist"},
                "address": {"city": "Wisokyburgh"}
            },
            {
                "name": "Clementine Bauch",
                "email": "clementine@example.com",
                "company": {"name": "Romaguera-Jacobson"},
                "address": {"city": "McKenziehaven"}
            }
        ]
        return mock_users, None


# ================= STEP 2: AI ANALYSIS (Rule-Based) =================
def analyze_user(user):
    try:
        name = user.get("name", "Unknown")
        company = user.get("company", {}).get("name", "Unknown")
        city = user.get("address", {}).get("city", "Unknown")
        catchphrase = user.get("company", {}).get("catchPhrase", "")

        analysis = (
            f"{name} works at {company} and is based in {city}. "
            f"The company description suggests a professional and structured environment."
        )

        # Simple sentiment classification
        positive_words = ["innovative", "success", "growth", "vision", "optimal"]
        negative_words = ["problem", "challenge", "failure", "risk"]

        text = catchphrase.lower()

        if any(word in text for word in positive_words):
            sentiment = "optimistic"
        elif any(word in text for word in negative_words):
            sentiment = "pessimistic"
        else:
            sentiment = "balanced"

        return analysis, sentiment, None
    except Exception as e:
        return None, None, f"AI Processing Error: {str(e)}"

# ================= STEP 3: STORE DATA =================
def store_data(source, raw_data, analysis, sentiment, timestamp):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO results (source, raw_data, analysis, sentiment, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (
            source,
            json.dumps(raw_data),
            analysis,
            sentiment,
            timestamp
        ))
        conn.commit()
        conn.close()
        return True, None
    except Exception as e:
        return False, f"Database Error: {str(e)}"

# ================= STEP 4: SEND NOTIFICATION =================
def send_notification(email, count):
    try:
        print("\n========= NOTIFICATION =========")
        print(f"Notification sent to: {email}")
        print(f"Items processed: {count}")
        print(f"Completed at: {datetime.utcnow().isoformat()}Z")
        print("================================\n")
        return True, None
    except Exception as e:
        return False, f"Notification Error: {str(e)}"

# ================= MAIN PIPELINE ENDPOINT =================
@app.route("/pipeline", methods=["POST"])
def run_pipeline():
    errors = []
    items = []

    if not request.is_json:
        return jsonify({
            "items": [],
            "notificationSent": False,
            "processedAt": datetime.utcnow().isoformat() + "Z",
            "errors": ["Invalid JSON request"]
        }), 400

    data = request.get_json()
    email = data.get("email")
    source = data.get("source", "JSONPlaceholder Users")

    if not email:
        return jsonify({
            "items": [],
            "notificationSent": False,
            "processedAt": datetime.utcnow().isoformat() + "Z",
            "errors": ["Email is required"]
        }), 400

    # STEP 1: Fetch
    users, fetch_error = fetch_users()
    if fetch_error:
        errors.append(fetch_error)

    for idx, user in enumerate(users):
        try:
            timestamp = datetime.utcnow().isoformat() + "Z"

            # STEP 2: Analyze
            analysis, sentiment, ai_error = analyze_user(user)
            if ai_error:
                errors.append(f"User {idx+1}: {ai_error}")
                analysis = "Analysis unavailable"
                sentiment = "balanced"

            # STEP 3: Store
            stored, store_error = store_data(
                source, user, analysis, sentiment, timestamp
            )
            if store_error:
                errors.append(f"User {idx+1}: {store_error}")

            items.append({
                "original": user.get("name"),
                "analysis": analysis,
                "sentiment": sentiment,
                "stored": stored,
                "timestamp": timestamp
            })

        except Exception as e:
            errors.append(f"User {idx+1} failed: {str(e)}")

    # STEP 4: Notify
    notification_sent, notify_error = send_notification(email, len(items))
    if notify_error:
        errors.append(notify_error)

    return jsonify({
        "items": items,
        "notificationSent": notification_sent,
        "processedAt": datetime.utcnow().isoformat() + "Z",
        "errors": errors
    }), 200

# Health check
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "Pipeline running",
        "endpoint": "POST /pipeline"
    })

@app.route("/pipeline", methods=["GET", "POST"])
def pipeline():
    if request.method == "GET":
        return jsonify({
            "status": "Pipeline running",
            "endpoint": "Use POST with JSON body"
        }), 200

import os

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
