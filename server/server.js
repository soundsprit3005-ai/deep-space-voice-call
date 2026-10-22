const WebSocket = require('ws');
const http = require('http');
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('Screen On Call signaling server is running'); return; }
  res.writeHead(404); res.end('Not found');
});
const wss = new WebSocket.Server({ server, path: '/ws' });
const rooms = new Map();
function send(ws, data) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data)); }
wss.on('connection', ws => {
  ws.room = null;
  ws.on('message', raw => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch { return send(ws, { type: 'error', message: 'Invalid message' }); }
    if (msg.type === 'join') {
      const room = String(msg.room || '').trim().toUpperCase();
      if (!/^[A-Z0-9-]{4,24}$/.test(room)) return send(ws, { type: 'error', message: 'Room code must be 4–24 letters/numbers/hyphens' });
      let members = rooms.get(room);
      if (!members) { members = new Set(); rooms.set(room, members); }
      if (members.size >= 2) return send(ws, { type: 'error', message: 'This room already has two people' });
      ws.room = room; members.add(ws); send(ws, { type: 'joined', room });
      // Only the already-waiting member initiates the offer; the newcomer waits for it.
      for (const other of members) if (other !== ws) send(other, { type: 'peer-joined' });
      return;
    }
    const members = ws.room && rooms.get(ws.room);
    if (!members) return send(ws, { type: 'error', message: 'Join a room first' });
    if (msg.type === 'leave') { cleanup(ws); return; }
    // Relay only within the sender's room; never trust a room value supplied in other messages.
    for (const other of members) if (other !== ws) send(other, msg);
  });
  ws.on('close', () => cleanup(ws));
  ws.on('error', () => cleanup(ws));
  function cleanup(client) {
    const room = client.room; if (!room) return;
    const members = rooms.get(room); if (members) {
      members.delete(client);
      for (const other of members) send(other, { type: 'peer-left' });
      if (!members.size) rooms.delete(room);
    }
    client.room = null;
  }
});
const port = Number(process.env.PORT || 8080);
server.listen(port, '0.0.0.0', () => console.log(`Signaling server listening on ${port}`));
