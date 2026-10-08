// TarjimCall signaling as a Cloudflare Worker (free serverless).
//
// Speaks EXACTLY the same WebSocket protocol as the home Node server, so the
// Android app needs no protocol change:
//   client -> server: register{number,name,token} | login{number} | search{query}
//                     | call{from,to} | join{room} | <relayed anything>
//   server -> client: registered | register-failed | login-ok | login-unknown
//                     | search-results{results} | call-ringing | call-unavailable
//                     | call-failed | joined{peers} | peer-joined | <relayed>
//
// All live state (sockets, rooms, registry) lives in ONE Durable Object so
// both phones always meet in the same place, and the registry survives
// hibernation/restarts via DO storage. Incoming-call push goes straight to
// FCM HTTP v1 (RS256 JWT signed with Web Crypto) — no firebase-admin needed.

export default {
  async fetch(request, env) {
    if (request.headers.get('Upgrade') === 'websocket') {
      const hub = env.HUB.get(env.HUB.idFromName('hub'));
      return hub.fetch(request);
    }
    if (new URL(request.url).pathname === '/health') {
      return json({ ok: true, service: 'tarjimcall-signaling' });
    }
    return new Response(
      'TarjimCall signaling Worker is up. Connect with a WebSocket client.',
      { headers: { 'Content-Type': 'text/plain' } }
    );
  },
};

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function b64urlBytes(bytes) {
  let bin = '';
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function b64urlText(text) {
  return b64urlBytes(new TextEncoder().encode(text));
}

function pemToBuffer(pem) {
  const b64 = pem.replace(/-----[^-]+-----/g, '').replace(/\s+/g, '');
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out.buffer;
}

export class Hub {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.rooms = new Map();     // room key -> Set<WebSocket>
    this.meta = new Map();      // WebSocket -> { number, name, room }
    this.registry = new Map();  // number -> { token, name, ts }
    this.registryReady = null;  // lazy load promise
    this.accessToken = null;    // { value, expiresAt }
  }

  // ---------------------------------------------------------------- storage
  loadRegistry() {
    if (!this.registryReady) {
      this.registryReady = this.state.storage.get('registry').then((saved) => {
        if (saved && typeof saved === 'object') {
          for (const [k, v] of Object.entries(saved)) this.registry.set(k, v);
        }
        return true;
      });
    }
    return this.registryReady;
  }

  persistRegistry() {
    const snapshot = Object.fromEntries(this.registry);
    this.state.storage.put('registry', snapshot).catch(() => {});
  }

  entryFor(number) {
    const v = this.registry.get(number);
    if (!v) return null;
    if (typeof v === 'string') return { token: v, name: '', ts: 0 };
    return v;
  }

  // ------------------------------------------------------------------ ws io
  send(ws, msg) {
    try {
      if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
    } catch (e) {
      // socket went away mid-send; close handler cleans up
    }
  }

  broadcast(room, sender, msg) {
    const peers = this.rooms.get(room);
    if (!peers) return;
    for (const peer of peers) if (peer !== sender) this.send(peer, msg);
  }

  async fetch(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    this.meta.set(server, { number: '', name: '', room: null });
    server.addEventListener('message', (ev) => {
      this.onMessage(server, ev.data).catch((e) => {
        console.error('handler error:', e && e.message);
      });
    });
    server.addEventListener('close', () => this.onClose(server));
    server.addEventListener('error', () => this.onClose(server));
    return new Response(null, { status: 101, webSocket: client });
  }

  onClose(ws) {
    const meta = this.meta.get(ws);
    this.meta.delete(ws);
    if (meta && meta.room) {
      const peers = this.rooms.get(meta.room);
      if (peers) {
        peers.delete(ws);
        if (!peers.size) this.rooms.delete(meta.room);
      }
    }
    // NOTE: the registry entry is kept on purpose so the number stays
    // reachable by push even while the phone is offline.
  }

  // -------------------------------------------------------------- protocol
  async onMessage(ws, raw) {
    let msg;
    try {
      msg = JSON.parse(typeof raw === 'string' ? raw : new TextDecoder().decode(raw));
    } catch (e) {
      return this.send(ws, { type: 'error', message: 'invalid message' });
    }
    const meta = this.meta.get(ws) || { number: '', name: '', room: null };

    // --- login / registration: number + name + token, once per device -----
    if (msg.type === 'register') {
      const number = String(msg.number || '').trim();
      const token = String(msg.token || '').trim();
      const name = String(msg.name || '').trim();
      if (!number || !token) {
        return this.send(ws, { type: 'register-failed', message: 'number and token required' });
      }
      await this.loadRegistry();
      this.registry.set(number, { token, name, ts: Date.now() });
      this.persistRegistry();
      meta.number = number;
      meta.name = name;
      console.log(`REGISTER ${number}${name ? ' (' + name + ')' : ''} (registry size ${this.registry.size})`);
      return this.send(ws, { type: 'registered', number, name });
    }

    // --- login lookup: does the hub already know this number? -------------
    if (msg.type === 'login') {
      const number = String(msg.number || '').trim();
      await this.loadRegistry();
      const entry = this.entryFor(number);
      if (!entry) return this.send(ws, { type: 'login-unknown', number });
      return this.send(ws, { type: 'login-ok', number, name: entry.name || '' });
    }

    // --- contact directory search: find registered users by name or number --
    // Any connected client may query the shared registry (this IS the contact
    // directory feature). Matches are substring, case-insensitive; capped at 25
    // so a one-letter query cannot dump the whole registry in one frame.
    if (msg.type === 'search') {
      const query = String(msg.query || '').trim().toLowerCase();
      await this.loadRegistry();
      if (!query) return this.send(ws, { type: 'search-results', query, results: [] });
      const results = [];
      for (const number of this.registry.keys()) {
        const entry = this.entryFor(number);
        const name = (entry && entry.name) ? String(entry.name) : '';
        if (name.toLowerCase().includes(query) || String(number).includes(query)) {
          results.push({ number: String(number), name });
          if (results.length >= 25) break;
        }
      }
      return this.send(ws, { type: 'search-results', query, results });
    }

    // --- contact matching: which of these numbers already have an account? ---
    // The app normalizes each address-book number on-device and sends the digit
    // list; we return only the subset present in the registry. Exact match
    // first, then a last-9-digits fallback (a contact may be stored in a
    // different local/international form than the owner registered). Response
    // is capped so a huge address book cannot blow up one frame.
    if (msg.type === 'match') {
      await this.loadRegistry();
      const digitsOf = (s) => String(s || '').replace(/\D/g, '');
      const bySuffix = new Map();
      for (const number of this.registry.keys()) {
        const d = digitsOf(number);
        if (d.length >= 9) {
          const entry = this.entryFor(number);
          bySuffix.set(d.slice(-9), {
            number: String(number),
            name: (entry && entry.name) ? String(entry.name) : '',
          });
        }
      }
      const incoming = Array.isArray(msg.numbers) ? msg.numbers : [];
      const seen = new Set();
      const results = [];
      for (const raw of incoming) {
        const number = String(raw || '').trim();
        if (!number) continue;
        let hitNumber = null;
        let hitName = '';
        const exact = this.entryFor(number);
        if (exact) {
          hitNumber = number;
          hitName = exact.name ? String(exact.name) : '';
        } else {
          const d = digitsOf(number);
          if (d.length >= 9) {
            const s = bySuffix.get(d.slice(-9));
            if (s) { hitNumber = s.number; hitName = s.name; }
          }
        }
        if (hitNumber && !seen.has(hitNumber)) {
          seen.add(hitNumber);
          results.push({ number: hitNumber, name: hitName });
          if (results.length >= 500) break;
        }
      }
      return this.send(ws, { type: 'match-results', results });
    }

    // --- call request: wake the callee by number via FCM ------------------
    if (msg.type === 'call') {
      const from = String(msg.from || '').trim();
      const to = String(msg.to || '').trim();
      if (!to) return this.send(ws, { type: 'call-failed', message: 'to is required' });
      await this.loadRegistry();
      const entry = this.entryFor(to);
      if (!entry || !entry.token) {
        console.log(`CALL ${from} -> ${to}: not registered`);
        return this.send(ws, { type: 'call-unavailable', to });
      }
      if (!this.env.FCM_SA) {
        console.log(`CALL ${from} -> ${to}: FCM secret missing`);
        return this.send(ws, { type: 'call-failed', to, reason: 'fcm-disabled' });
      }
      try {
        await this.pushCallInvite(entry.token, from, to, this.entryFor(from)?.name || '');
        console.log(`CALL ${from} -> ${to}: push sent`);
        return this.send(ws, { type: 'call-ringing', to });
      } catch (e) {
        const detail = String(e && e.message ? e.message : e);
        // A dead token should not stay in the registry forever.
        if (/REGISTRATION_TOKEN_NOT_REGISTERED|registration-token-not-registered|UNREGISTERED/i.test(detail)) {
          this.registry.delete(to);
          this.persistRegistry();
        }
        console.error(`CALL ${from} -> ${to}: push failed`, detail);
        return this.send(ws, { type: 'call-failed', to, reason: detail });
      }
    }

    // --- room join (live relay) -------------------------------------------
    if (msg.type === 'join') {
      const room = String(msg.room || '');
      if (!room) return this.send(ws, { type: 'error', message: 'room is required' });
      if (meta.room && meta.room !== room) {
        const old = this.rooms.get(meta.room);
        if (old) {
          old.delete(ws);
          if (!old.size) this.rooms.delete(meta.room);
        }
      }
      meta.room = room;
      if (!this.rooms.has(room)) this.rooms.set(room, new Set());
      const peers = this.rooms.get(room);
      this.send(ws, { type: 'joined', peers: peers.size });
      for (const peer of peers) this.send(peer, { type: 'peer-joined' });
      peers.add(ws);
      return;
    }

    // --- server-side translation fallback ---------------------------------
    // On-device ML Kit models cannot download on some networks (or on
    // devices without Play services). The call socket is already connected
    // here, so translate server-side and answer on the same socket: the
    // call keeps translating on ANY network, no phone setup required.
    if (msg.type === 'translate-req') {
      const text = String(msg.text || '').slice(0, 2000);
      const from = String(msg.from || 'auto');
      const to = String(msg.to || 'en');
      const rid = String(msg.rid || '');
      if (!text.trim()) {
        return this.send(ws, { type: 'translate-res', rid, ok: false, text: '' });
      }
      const out = await cloudTranslate(text, from, to);
      console.log(`TRANSLATE ${from}->${to} rid=${rid} ok=${!!out}`);
      return this.send(ws, { type: 'translate-res', rid, ok: !!out, text: out || '' });
    }

    // --- everything else is relayed inside the room -----------------------
    if (meta.room) this.broadcast(meta.room, ws, msg);
  }

  // ------------------------------------------------------------------- FCM
  async getAccessToken(sa) {
    if (this.accessToken && this.accessToken.expiresAt > Date.now() + 60_000) {
      return this.accessToken.value;
    }
    const now = Math.floor(Date.now() / 1000);
    const header = b64urlText(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
    const claims = b64urlText(JSON.stringify({
      iss: sa.client_email,
      scope: 'https://www.googleapis.com/auth/firebase.messaging',
      aud: 'https://oauth2.googleapis.com/token',
      iat: now,
      exp: now + 3600,
    }));
    const unsigned = `${header}.${claims}`;
    const key = await crypto.subtle.importKey(
      'pkcs8',
      pemToBuffer(sa.private_key),
      { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
      false,
      ['sign']
    );
    const sig = await crypto.subtle.sign(
      'RSASSA-PKCS1-v1_5',
      key,
      new TextEncoder().encode(unsigned)
    );
    const jwt = `${unsigned}.${b64urlBytes(new Uint8Array(sig))}`;

    const res = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body:
        'grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=' +
        encodeURIComponent(jwt),
    });
    if (!res.ok) throw new Error(`FCM token exchange failed: ${res.status} ${await res.text()}`);
    const j = await res.json();
    this.accessToken = {
      value: j.access_token,
      expiresAt: Date.now() + (Number(j.expires_in || 3600) * 1000),
    };
    return this.accessToken.value;
  }

  async pushCallInvite(token, from, to, fromName) {
    const sa = JSON.parse(this.env.FCM_SA);
    const accessToken = await this.getAccessToken(sa);
    const res = await fetch(
      `https://fcm.googleapis.com/v1/projects/${sa.project_id}/messages:send`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${accessToken}`,
        },
        body: JSON.stringify({
          message: {
            token,
            // "from"/"to" are reserved FCM envelope keys: putting them inside
            // data makes messages:send reject the payload with 400
            // INVALID_ARGUMENT "Invalid data payload key: from".
            data: {
              type: 'call-invite',
              caller: String(from || ''),
              callee: String(to || ''),
              name: String(fromName || ''),
              ts: String(Date.now()),
            },
            android: { priority: 'high', ttl: '60s' },
          },
        }),
      }
    );
    if (!res.ok) throw new Error(`FCM send failed: ${res.status} ${await res.text()}`);
    return true;
  }
}

// Server-side translation for phones that cannot run ML Kit on-device
// (model download blocked by the network, no Play services, ...). Tries
// Google's public translate endpoint first, then the free MyMemory API.
async function cloudTranslate(text, from, to) {
  try {
    const url =
      'https://translate.googleapis.com/translate_a/single?client=gtx&dt=t' +
      '&sl=' + encodeURIComponent(from) +
      '&tl=' + encodeURIComponent(to) +
      '&q=' + encodeURIComponent(text);
    const res = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Linux; Android 14)' },
    });
    if (res.ok) {
      const j = await res.json();
      const segs = Array.isArray(j) && Array.isArray(j[0]) ? j[0] : [];
      const out = segs
        .map((s) => (Array.isArray(s) ? s[0] : ''))
        .filter(Boolean)
        .join('');
      if (out.trim()) return out;
    }
  } catch (e) {
    console.error('cloudTranslate gtx failed', e && e.message);
  }
  try {
    const res = await fetch(
      'https://api.mymemory.translated.net/get?q=' + encodeURIComponent(text) +
        '&langpair=' + encodeURIComponent(from + '|' + to)
    );
    if (res.ok) {
      const j = await res.json();
      const out = j && j.responseData && j.responseData.translatedText;
      if (typeof out === 'string' && out.trim()) return out;
    }
  } catch (e) {
    console.error('cloudTranslate mymemory failed', e && e.message);
  }
  return '';
}
