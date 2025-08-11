const express = require('express');
const cors = require('cors');

const app = express();
const port = 3001; // As per convention for log servers

// In-memory store for logs
let logs = [];

app.use(cors());
app.use(express.json({ limit: '10mb' })); // Allow larger log payloads

// Endpoint to receive logs from other services
app.post('/logs', (req, res) => {
    const { source, level, message, timestamp } = req.body;
    if (!source || !level || !message || !timestamp) {
        return res.status(400).send({ error: 'Log message must include source, level, message, and timestamp.' });
    }
    const logEntry = { source, level, message, timestamp, receivedAt: new Date().toISOString() };
    logs.push(logEntry);
    console.log(`[${level.toUpperCase()}] from ${source}: ${message}`);
    res.status(202).send({ status: 'accepted' });
});

// Endpoint for the log viewer to fetch logs
app.get('/logs', (req, res) => {
    res.json(logs);
});

// A simple HTML log viewer
app.get('/', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Project Chronos - Unified Log Viewer</title>
            <style>
                body { font-family: monospace; background-color: #1a1a1a; color: #f0f0f0; margin: 0; padding: 20px; }
                h1 { color: #4CAF50; }
                #log-container { white-space: pre-wrap; word-wrap: break-word; }
                .log-entry { margin-bottom: 5px; padding: 10px; border-radius: 5px; }
                .log-entry.info { background-color: #2e2e2e; border-left: 5px solid #0096FF; }
                .log-entry.warn { background-color: #3e3e2e; border-left: 5px solid #FFC107; }
                .log-entry.error { background-color: #4e2e2e; border-left: 5px solid #F44336; }
                .log-source { font-weight: bold; color: #4CAF50; }
                .log-timestamp { color: #aaa; }
            </style>
        </head>
        <body>
            <h1>Project Chronos - Unified Log Viewer</h1>
            <div id="filters">
                <label for="source-filter">Filter by source:</label>
                <input type="text" id="source-filter" onkeyup="filterLogs()">
                <label for="level-filter">Filter by level:</label>
                <select id="level-filter" onchange="filterLogs()">
                    <option value="">All</option>
                    <option value="info">INFO</option>
                    <option value="warn">WARN</option>
                    <option value="error">ERROR</option>
                </select>
            </div>
            <div id="log-container"></div>

            <script>
                const logContainer = document.getElementById('log-container');
                const sourceFilter = document.getElementById('source-filter');
                const levelFilter = document.getElementById('level-filter');
                let allLogs = [];

                function renderLogs() {
                    logContainer.innerHTML = '';
                    const sourceValue = sourceFilter.value.toLowerCase();
                    const levelValue = levelFilter.value;

                    const filteredLogs = allLogs.filter(log => {
                        const sourceMatch = log.source.toLowerCase().includes(sourceValue);
                        const levelMatch = !levelValue || log.level.toLowerCase() === levelValue;
                        return sourceMatch && levelMatch;
                    });

                    filteredLogs.forEach(log => {
                        const logElement = document.createElement('div');
                        logElement.className = 'log-entry ' + log.level.toLowerCase();
                        logElement.innerHTML = \`
                            <span class="log-timestamp">[\${log.timestamp}]</span>
                            [<span class="log-source">\${log.source}</span>]
                            [\${log.level.toUpperCase()}]
                            - \${log.message}
                        \`;
                        logContainer.appendChild(logElement);
                    });
                }

                function filterLogs() {
                    renderLogs();
                }

                async function fetchLogs() {
                    try {
                        const response = await fetch('/logs');
                        allLogs = await response.json();
                        renderLogs();
                    } catch (error) {
                        console.error('Error fetching logs:', error);
                    }
                }

                // Fetch logs every 2 seconds
                setInterval(fetchLogs, 2000);
                // Initial fetch
                fetchLogs();
            </script>
        </body>
        </html>
    `);
});

app.listen(port, () => {
    console.log(`Unified Log Server listening at http://localhost:${port}`);
});
