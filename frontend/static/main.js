let map;
let markers = [];
let stChart;

function getWeatherIcon(condition) {
    const icons = { "Cerah": `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2"/><path d="M12 20v2"/><path d="M2 12h2"/><path d="M20 12h2"/></svg>` };
    return icons[condition] || icons["Cerah"];
}

async function loadData() {
    try {
        const res = await fetch("/api/recommendations");
        const data = await res.json();
        
        const st = data.screen_time; 
        const weather = data.weather;
        const recs = data.recommendations;
        const userLoc = data.user_location;

        const hours = Math.floor(st / 60);
        const minutes = st % 60;
        document.getElementById('st-val').innerText = `${hours} Jam ${minutes} Menit`;
        document.getElementById('weather-val').innerText = weather.condition;
        document.getElementById('temp-val').innerText = weather.temperature;
        document.getElementById('weather-icon').innerHTML = getWeatherIcon(weather.condition);
        
        updateChart(data.screen_time_history);

        const stIndicator = document.getElementById('st-indicator'); 
        const recommendedSection = document.getElementById('recommended-section');
        const altSection = document.getElementById('alt-section');
        const mapSection = document.getElementById('map-section');
        const recDiv = document.getElementById("recommended-list");
        const waitDiv = document.getElementById("wait-list");

        recDiv.innerHTML = ""; 
        waitDiv.innerHTML = "";

        if (st < 480) { 
            if (stIndicator) { 
                stIndicator.innerText = "Status: Aman"; 
                stIndicator.className = "mt-2 text-xs font-bold text-blue-600 tracking-tight"; 
            }
            recommendedSection.classList.remove('hidden');
            recDiv.innerHTML = `<div class="bg-white border border-slate-200 p-12 text-center rounded-3xl shadow-sm"><p class="text-slate-900 font-bold text-lg leading-relaxed">Screen time Anda masih di bawah ambang batas.<br><span class="text-slate-500 font-medium text-base">Tidak usah ke mana-mana dulu, silakan lanjutkan aktivitas Anda.</span></p></div>`;
            altSection.classList.add('hidden');
            if (mapSection) mapSection.classList.add('hidden');
        } else {
            if (stIndicator) { 
                stIndicator.innerText = "Status: Perlu Istirahat"; 
                stIndicator.className = "mt-2 text-xs font-bold text-emerald-600 tracking-tight"; 
            }
            
            const recommendedItems = recs.filter(r => r.final_decision === 'RECOMMENDED').slice(0, 5);
            const waitItems = recs.filter(r => r.final_decision === 'WAIT').slice(0, 3);

            if (recommendedItems.length === 0) {
                recommendedSection.classList.add('hidden');
            } else {
                recommendedSection.classList.remove('hidden');
                recommendedItems.forEach(r => { recDiv.innerHTML += createLargeCard(r); });
            }

            if (waitItems.length > 0) {
                altSection.classList.remove('hidden');
                waitItems.forEach(r => { waitDiv.innerHTML += createSmallCard(r); });
            } else {
                altSection.classList.add('hidden');
            }

            if (mapSection) mapSection.classList.remove('hidden');
            updateMap(recs, userLoc);
        }
    } catch (e) { console.error("Error load data:", e); }
}

function updateChart(history) {
    const ctx = document.getElementById('screenTimeChart').getContext('2d');
    const labels = history.map(h => {
        const [year, month, day] = h.time.split('-');
        return `${day}-${month}-${year}`;
    });
    const values = history.map(h => (h.value / 60).toFixed(1));

    if (!stChart) {
        stChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Durasi (Jam)',
                    data: values,
                    backgroundColor: '#0f172a',
                    borderRadius: 10,
                    borderSkipped: false,
                    barPercentage: 0.6,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { 
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return context.parsed.y + ' jam';
                            }
                        },
                        backgroundColor: '#1e293b',
                        padding: 12,
                        cornerRadius: 8,
                        displayColors: false
                    }
                },
                scales: {
                    y: { 
                        beginAtZero: true, 
                        title: {
                            display: true,
                            text: 'Jam',
                            font: { weight: 'bold' }
                        },
                        grid: { 
                            display: true,
                            color: '#f1f5f9',
                            drawBorder: false
                        }, 
                        ticks: { 
                            font: { weight: '600', size: 11 },
                            color: '#64748b'
                        } 
                    },
                    x: { 
                        grid: { display: false }, 
                        ticks: { 
                            font: { weight: '600', size: 11 },
                            color: '#64748b'
                        } 
                    }
                }
            }
        });
    } else {
        stChart.data.labels = labels;
        stChart.data.datasets[0].data = values;
        stChart.update();
    }
}

function focusOnMap(lat, lng) {
    document.getElementById('map-section').scrollIntoView({ behavior: 'smooth' });
    if (map) { map.setView([lat, lng], 16); markers.forEach(m => { if (m.getLatLng().lat === lat && m.getLatLng().lng === lng) m.openPopup(); }); }
}

function createLargeCard(r) {
    return `<div class="bg-white rounded-3xl border border-l-[12px] border-l-emerald-500 p-8 shadow-sm">
        <div class="flex justify-between items-start mb-6">
            <div><h3 class="font-bold text-slate-900 text-2xl mb-1">${r.place_name}</h3><span class="text-[10px] font-black text-slate-400 uppercase tracking-widest">${r.category}</span></div>
            <div class="text-right"><span class="block text-4xl font-black text-emerald-600 tracking-tighter">${r.score}</span><span class="text-[10px] text-slate-900 font-bold uppercase">Match Score</span></div>
        </div>
        <div class="flex items-baseline gap-3 mb-6"><span class="text-6xl font-black text-slate-900 tracking-tighter">${r.distance_km}</span><span class="text-lg font-bold text-slate-400 font-medium">km dari lokasi Anda</span></div>
        <div class="flex justify-between items-end gap-4 mb-6"><p class="text-sm text-slate-600 font-bold max-w-[60%]">${r.address}</p><button onclick="focusOnMap(${r.latitude}, ${r.longitude})" class="bg-slate-900 text-white px-6 py-3 rounded-2xl text-xs font-bold shrink-0">Lihat Peta</button></div>
        <div class="bg-slate-50 border p-6 rounded-2xl"><p class="text-slate-700 font-bold text-sm">"${r.decision_reason}"</p></div>
    </div>`;
}

function createSmallCard(r) {
    return `<div class="bg-white rounded-2xl border border-l-[6px] border-l-amber-400 p-4 shadow-sm">
        <div class="flex justify-between items-start mb-2"><h3 class="font-bold text-slate-900 text-base leading-tight">${r.place_name}</h3><span class="text-xl font-black text-amber-600">${r.score}</span></div>
        <div class="font-bold mb-2"><span class="text-2xl">${r.distance_km}</span> <span class="text-[10px]">km</span></div>
        <button onclick="focusOnMap(${r.latitude}, ${r.longitude})" class="text-[10px] text-blue-600 font-bold uppercase hover:underline mb-2">Lihat Peta</button>
        <div class="bg-slate-50 p-3 rounded-xl text-[10px] text-slate-600 font-bold">"${r.decision_reason}"</div>
    </div>`;
}

function updateMap(recs, userLoc) {
    if (!map) { map = L.map('map').setView([userLoc.lat, userLoc.lng], 13); L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map); }
    markers.forEach(m => map.removeLayer(m)); markers = [];
    L.circleMarker([userLoc.lat, userLoc.lng], { color: '#3b82f6', radius: 8 }).addTo(map).bindPopup('<b>Lokasi Anda</b>');
    recs.forEach(r => {
        const popup = `<div class="p-1"><b>${r.place_name}</b><br><a href="https://www.google.com/maps/search/?api=1&query=${r.latitude},${r.longitude}" target="_blank" class="text-blue-600 font-bold text-xs">Open with Google Maps</a></div>`;
        const m = L.marker([r.latitude, r.longitude]).addTo(map).bindPopup(popup); markers.push(m);
    });
}

document.addEventListener("DOMContentLoaded", () => { loadData(); setInterval(loadData, 10000); });