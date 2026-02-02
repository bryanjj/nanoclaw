/**
 * Dashboard Server
 * HTTP + WebSocket server for real-time dashboard
 */

import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import fs from 'fs';
import path from 'path';
import pino from 'pino';
import { dashboardEvents, DashboardEvent } from './dashboard-events.js';
import { DASHBOARD_PORT } from './config.js';

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: { target: 'pino-pretty', options: { colorize: true } }
});

export function startDashboardServer(): void {
  const server = createServer((req, res) => {
    // Serve static dashboard HTML
    if (req.url === '/' || req.url === '/dashboard') {
      const htmlPath = path.join(process.cwd(), 'dashboard', 'index.html');
      if (fs.existsSync(htmlPath)) {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(fs.readFileSync(htmlPath));
      } else {
        res.writeHead(404);
        res.end('Dashboard not found. Create dashboard/index.html');
      }
      return;
    }
    res.writeHead(404);
    res.end('Not found');
  });

  const wss = new WebSocketServer({ server });

  wss.on('connection', (ws) => {
    logger.debug('Dashboard client connected');

    // Send recent events on connect
    const recent = dashboardEvents.getRecentEvents();
    ws.send(JSON.stringify({ type: 'history', events: recent }));

    const handler = (event: DashboardEvent) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'event', event }));
      }
    };

    dashboardEvents.on('dashboard', handler);
    ws.on('close', () => {
      dashboardEvents.off('dashboard', handler);
      logger.debug('Dashboard client disconnected');
    });
  });

  server.listen(DASHBOARD_PORT, () => {
    logger.info({ port: DASHBOARD_PORT }, 'Dashboard server started');
  });
}
