const express = require('express');
const cors = require('cors');
const axios = require('axios');
const path = require('path');
const app = express();

// --- CONFIGURATION ---
// In a real production environment, use process.env.RFMS_STORE_ID
const STORE_QUEUE = process.env.STORE_QUEUE || "store-0fe6363b18104aefad0e938161612704";
const API_KEY = process.env.API_KEY || "a405f5ca9152402297dd513dfd2b42a0";
const RFMS_BASE_URL = "https://api.rfms.online/v2";

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// 1. AUTHENTICATION PROXY
// The frontend calls this to get a session token without exposing the API Key
app.post('/api/auth', async (req, res) => {
    try {
        const credentials = Buffer.from(`${STORE_QUEUE}:${API_KEY}`).toString('base64');
        const response = await axios.post(`${RFMS_BASE_URL}/session/begin`, {}, {
            headers: { 'Authorization': `Basic ${credentials}` }
        });
        res.json(response.data);
    } catch (error) {
        console.error("Auth Error:", error.response?.data || error.message);
        res.status(error.response?.status || 500).json(error.response?.data || { error: "Auth Failed" });
    }
});

// 2. GENERAL API PROXY
// All other requests to /api/rfms/... are forwarded to RFMS
// The frontend must provide the 'x-rfms-token' header which we convert to Basic Auth
app.use('/api/rfms', async (req, res) => {
    const token = req.headers['x-rfms-token'];

    if (!token) {
        return res.status(401).json({ error: "Missing session token" });
    }

    // Construct the URL to forward to
    // e.g. /api/rfms/opportunities/Measure -> https://api.rfms.online/v2/opportunities/Measure
    const endpoint = req.url; 
    const targetUrl = `${RFMS_BASE_URL}${endpoint}`;

    // Create Basic Auth header using StoreQueue + SessionToken
    const authHeader = `Basic ${Buffer.from(`${STORE_QUEUE}:${token}`).toString('base64')}`;

    try {
        const response = await axios({
            method: req.method,
            url: targetUrl,
            headers: { 
                'Authorization': authHeader,
                'Content-Type': 'application/json'
            },
            data: req.body
        });
        res.json(response.data);
    } catch (error) {
        console.error("Proxy Error:", error.response?.data || error.message);
        res.status(error.response?.status || 500).json(error.response?.data || { error: "Request Failed" });
    }
});

// Serve the frontend
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
