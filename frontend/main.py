from flask import Flask, render_template, jsonify

# delete this line after testing
Screen_time = 1
USER_LAT = -3.3225
USER_LNG = 114.5940

WEATHER_DATA = {
    "condition": "Cerah",
    "temperature": "32°C",
    "humidity": "65%"
}

HISTORY = [
        {"time": "08:00", "value": 60},
        {"time": "10:00", "value": 180},
        {"time": "12:00", "value": 300},
        {"time": "14:00", "value": 420},
        {"time": "16:00", "value": 540}
    ]

DUMMY_RECOMMENDATIONS = [
    {
        "place_id": 8,
        "place_name": "Taman Kamboja",
        "latitude": -3.3221064,
        "longitude": 114.5872086,
        "category": "park",
        "address": "Jl. H. Anang Adenansi, Banjarmasin Tengah",
        "distance_km": 1.2,
        "score": 9.2,
        "final_decision": "RECOMMENDED",
        "decision_reason": "Udara segar dan jarak dekat sangat baik untuk mata Anda."
    },
    {
        "place_id": 1,
        "place_name": "Open Space ULM",
        "latitude": -3.2973146,
        "longitude": 114.5868619,
        "category": "park",
        "address": "Jl. Brigjen H. Hasan Basry, Banjarmasin Utara",
        "distance_km": 2.1,
        "score": 8.7,
        "final_decision": "RECOMMENDED",
        "decision_reason": "Area terbuka hijau dengan keramaian rendah."
    },
    {
        "place_id": 2,
        "place_name": "NORDEN Coffee",
        "latitude": -3.2975807,
        "longitude": 114.5876179,
        "category": "cafe",
        "address": "Jl. Brigjen H. Hasan Basry No. 46",
        "distance_km": 0.9,
        "score": 7.1,
        "final_decision": "RECOMMENDED",
        "decision_reason": "Opsi alternatif jika Anda ingin suasana berbeda."
    }
]

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route("/api/recommendations")
def api_recommendations():

    data = sorted(
        DUMMY_RECOMMENDATIONS,
        key=lambda x: (0 if x["final_decision"] == "RECOMMENDED" else 1, -x["score"])
    )

    return jsonify({
        "screen_time": Screen_time,
        "recommendations": data,
        "weather": WEATHER_DATA,
        "screen_time_history": HISTORY,
        "user_location": {"lat": USER_LAT, "lng": USER_LNG},
    })

if __name__ == "__main__":
    app.run(debug=True)
