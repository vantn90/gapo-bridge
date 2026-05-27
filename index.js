const express = require('express');
const WebSocket = require('ws');
const axios = require('axios');

const app = express();
app.use(express.json());

const GOCLAW_WS       = process.env.GOCLAW_WS       || 'ws://goclaw-goclaw-1:8080/ws';
const GOCLAW_TOKEN    = process.env.GOCLAW_TOKEN;
const GOCLAW_AGENT_ID = process.env.GOCLAW_AGENT_ID || '1';
const GAPO_BASE       = 'https://api.gapowork.vn/3rd-bot/v1.0';
const GAPO_BOT_ID     = process.env.GAPO_BOT_ID;
const GAPO_TOKEN      = process.env.GAPO_TOKEN;

const sessions = new Map();

function connectGoClaw(userId) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(GOCLAW_WS);

    ws.once('open', () => {
      ws.send(JSON.stringify({
        type: 'req', id: 'init', method: 'connect',
        params: { token: GOCLAW_TOKEN, user_id: userId, protocol: 3 }
      }));
    });

    ws.once('message', (data) => {
      const msg = JSON.parse(data);
      if (msg.id === 'init') resolve(ws);
      else reject(new Error('Connect failed: ' + JSON.stringify(msg)));
    });

    ws.once('error', reject);
    setTimeout(() => reject(new Error('WS connect timeout')), 10000);
  });
}

async function sendToGoClaw(userId, text) {
  let ws = sessions.get(userId);
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    ws = await connectGoClaw('gapo-bridge-' + userId);
    sessions.set(userId, ws);
    ws.on('close', () => sessions.delete(userId));
  }

  return new Promise((resolve) => {
    const reqId = 'msg-' + Date.now();
    let fullText = '';

    ws.send(JSON.stringify({
      type: 'req', id: reqId, method: 'chat.send',
      params: {
        message: text,
        agentId: GOCLAW_AGENT_ID,
        sessionKey: 'gapo.user.' + userId
      }
    }));

    const onMessage = (data) => {
      try {
        const msg = JSON.parse(data);
        if (msg.type === 'event' && msg.event === 'agent') {
          if (msg.payload.type === 'run.completed') {
            ws.removeListener('message', onMessage);
            resolve(msg.payload.payload?.content || fullText);
          } else if (msg.payload.type === 'chunk') {
            const chunk = msg.payload.payload?.content;
            if (chunk) fullText += chunk;
          }
        }
      } catch (_) {}
    };

    ws.on('message', onMessage);
    setTimeout(() => {
      ws.removeListener('message', onMessage);
      resolve(fullText || '(timeout)');
    }, 60000);
  });
}

async function sendToGapo(partnerId, text) {
  try {
    await axios.post(
      GAPO_BASE + '/actions/messages',
      { partner_id: String(partnerId), message: { type: 'text', text } },
      { headers: { 'Authorization': 'Bot ' + GAPO_BOT_ID + ':' + GAPO_TOKEN } }
    );
  } catch (err) {
    const detail = err.response ? JSON.stringify(err.response.data) : err.message;
    throw new Error('GAPO API ' + (err.response?.status || '') + ': ' + detail);
  }
}

app.post('/webhook', async (req, res) => {
  console.log('GAPO webhook:', JSON.stringify(req.body, null, 2));
  res.json({ ok: true });

  try {
    const body = req.body;
    const senderId = body?.from_user_id;
    const text     = body?.message?.text;

    if (!senderId || !text) {
      console.log('Cannot parse senderId/text, skipping');
      return;
    }

    console.log('[' + senderId + '] -> GoClaw:', text);
    const reply = await sendToGoClaw(String(senderId), text);
    console.log('[' + senderId + '] <- GoClaw:', reply.substring(0, 120));
    await sendToGapo(senderId, reply);
  } catch (err) {
    console.error('Webhook error:', err.message);
  }
});

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log('gapo-bridge listening on :' + PORT));
