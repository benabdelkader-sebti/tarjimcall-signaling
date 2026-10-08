const PRIVACY_POLICY_HTML = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>TarjimCall Privacy Policy</title><style>body{font-family:system-ui,sans-serif;max-width:850px;margin:40px auto;padding:0 20px;line-height:1.7}h1,h2{color:#00796b}.ar{direction:rtl;text-align:right}</style></head><body>
<h1>TarjimCall — Privacy Policy</h1>
<p><strong>Effective date: 2026-10-05</strong></p>
<h2>English</h2>
<p>TarjimCall processes speech on the user's device using Android Speech Recognition and translates it on-device with Google ML Kit. During a call, recognized text is encrypted end-to-end with an ephemeral per-call key before it is relayed between the two participants. The signaling relay does not receive plaintext conversation text and does not store translated messages.</p>
<p>The service may process your phone number, optional display name and Firebase Cloud Messaging token to register the account, find users and deliver incoming-call notifications. Optional contact matching sends normalized phone-number digits for membership matching.</p>
<p>We do not sell conversation data. Crash reports are stored locally unless the user explicitly exports them. Account data is retained while needed for call routing and may be deleted on request.</p>
<p>Service providers include Google Firebase Cloud Messaging, Google ML Kit, Android Speech Recognition and Cloudflare Workers.</p>
<p>Contact: <a href="mailto:benabdelkader506@gmail.com">benabdelkader506@gmail.com</a></p>
<hr>
<div class="ar" lang="ar"><h2>سياسة الخصوصية</h2>
<p>يعالج TarjimCall الكلام على جهاز المستخدم باستخدام التعرف على الكلام في Android، وتتم الترجمة محلياً باستخدام Google ML Kit. أثناء المكالمة يتم تشفير النص المتعرّف عليه من طرف إلى طرف بمفتاح مؤقت خاص بالمكالمة قبل تمريره بين الطرفين. وسيط الإشارات لا يستقبل نص المحادثة بصيغته الواضحة ولا يخزن الرسائل المترجمة.</p>
<p>قد نعالج رقم الهاتف والاسم الاختياري ورمز Firebase لإدارة الحسابات والعثور على المستخدمين وإيصال إشعارات المكالمات الواردة. ميزة مطابقة جهات الاتصال الاختيارية ترسل أرقام الهواتف المطبّعة فقط للمطابقة.</p>
<p>لا نبيع بيانات المحادثات. وتبقى تقارير الأعطال محلياً على الجهاز ما لم يقم المستخدم بتصديرها. تُحتفظ ببيانات الحساب طالما كانت لازمة لتوجيه المكالمات ويمكن طلب حذفها.</p>
<p>للتواصل: <a href="mailto:benabdelkader506@gmail.com">benabdelkader506@gmail.com</a></p></div>
</body></html>`;

// TarjimCall signaling as a Cloudflare Worker (free serverless).
//
// Speaks the WebSocket protocol the Android app expects. Every extra field
// (attempt, expiresAt, callee) is optional, so an older installed app build and
// an older deployed Worker keep working together while either side updates.
//   client -> server: register{number,name,token} | login{number} | search{query}
//                     | match{numbers} | call{from,to,attempt,expiresAt}
//                     | call-cancel{attempt,to} | join{room} | <relayed anything>
//   server -> client: registered | register-failed | login-ok | login-unknown
//                     | search-results{results} | match-results{results}
//                     | call-ringing{to,callee,attempt} | call-unavailable
//                     | call-failed | call-cancel | joined{peers} | peer-joined
//                     | <relayed>
//
// All live state (sockets, rooms, registry) lives in ONE Durable Object so
// both phones always meet in the same place, and the registry survives
// hibernation/restarts via DO storage. Incoming-call push goes straight to
// FCM HTTP v1 (RS256 JWT signed with Web Crypto) — no firebase-admin needed.

// Max time an incoming-call push may stay alive. The caller stops ringing at
// 45s, so anything longer can only wake the callee for a dead call.
const INVITE_MAX_LIFETIME_MS = 40000;

export default {
  async fetch(request, env) {
    if (request.headers.get('Upgrade') === 'websocket') {
      const hub = env.HUB.get(env.HUB.idFromName('hub'));
      return hub.fetch(request);
    }
    if (request.method === 'GET' && new URL(request.url).pathname === '/privacy-policy') {
      return new Response(PRIVACY_POLICY_HTML, {
        headers: { 'Content-Type': 'text/html; charset=UTF-8', 'Cache-Control': 'public, max-age=3600' }
      });
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
    this.pending = new Map();   // attempt id -> { to, expiresAt }
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

  // Awaited by register() so the ack only reaches the phone once the token is
  // durably stored; fire-and-forget callers just get a never-rejecting promise.
  persistRegistry() {
    const snapshot = Object.fromEntries(this.registry);
    return this.state.storage.put('registry', snapshot).catch(() => {});
  }

  entryFor(number) {
    const v = this.registry.get(number);
    if (!v) return null;
    if (typeof v === 'string') return { token: v, name: '', ts: 0 };
    return v;
  }

  // A caller may dial the local form (0773…) while the callee registered the
  // international one (213773…), because each phone canonicalizes with its own
  // SIM country. Exact key first, then the same last-9-digits fallback the
  // contact matcher already uses, so both forms reach the same phone.
  resolveEntry(number) {
    const clean = String(number || '').trim();
    const direct = this.entryFor(clean);
    if (direct) return { entry: direct, number: clean };

    const digits = clean.replace(/\D/g, '');
    if (digits.length >= 9) {
      const suffix = digits.slice(-9);
      for (const key of this.registry.keys()) {
        const d = String(key).replace(/\D/g, '');
        if (d.length >= 9 && d.slice(-9) === suffix) {
          return { entry: this.entryFor(key), number: String(key) };
        }
      }
    }
    return { entry: null, number: '' };
  }

  // Attempts still ringing, so a "call-cancel" can find the callee even if the
  // caller sends only the attempt id. Bounded and pruned: a Worker restart
  // simply loses them, and the invite's own expiry covers that case.
  rememberAttempt(attempt, to, expiresAt) {
    if (!attempt) return;
    this.pending.set(attempt, { to, expiresAt });
    const now = Date.now();
    for (const [id, p] of this.pending) if (p.expiresAt < now) this.pending.delete(id);
    while (this.pending.size > 200) this.pending.delete(this.pending.keys().next().value);
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
      // The ack must come after the write: the app treats "registered" as proof
      // it is reachable by push, so a lost write would leave a number that looks
      // online forever while every incoming call to it fails.
      await this.persistRegistry();
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
      const { entry, number } = this.resolveEntry(to);
      if (!entry || !entry.token) {
        console.log(`CALL ${from} -> ${to}: not registered`);
        return this.send(ws, { type: 'call-unavailable', to });
      }
      if (!this.env.FCM_SA) {
        console.log(`CALL ${from} -> ${to}: FCM secret missing`);
        return this.send(ws, { type: 'call-failed', to, reason: 'fcm-disabled' });
      }
      // Short lifetime on purpose: the caller gives up at 45s, and an invite
      // delivered after that makes the callee ring for a call that no longer
      // exists. The app also drops the invite past this timestamp. Clamped at
      // both ends because a phone with a skewed clock must not kill its own call.
      const nowMs = Date.now();
      const wanted = Number(msg.expiresAt) || nowMs + INVITE_MAX_LIFETIME_MS;
      const expiresAt = Math.min(Math.max(wanted, nowMs + 5000), nowMs + INVITE_MAX_LIFETIME_MS);
      const attempt = String(msg.attempt || '').trim() ||
        `${from || 'call'}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      this.rememberAttempt(attempt, number, expiresAt);
      try {
        await this.pushCallInvite(entry.token, from, number, this.entryFor(from)?.name || '', {
          attempt,
          expiresAt,
        });
        console.log(`CALL ${from} -> ${number}: push sent`);
        return this.send(ws, { type: 'call-ringing', to, callee: number, attempt });
      } catch (e) {
        const detail = String(e && e.message ? e.message : e);
        // A dead token must not stay in the registry: FCM would keep reporting
        // success while the phone never rings, which looks exactly like a call
        // that only works one way. v1 words this as "Registration token is not
        // registered" or the canonical UNREGISTERED code; legacy wording matches too.
        if (/UNREGISTERED|NOT\s+REGISTERED|SENDER_ID_MISMATCH/i.test(detail)) {
          this.registry.delete(number);
          this.persistRegistry();
          console.log(`CALL ${from} -> ${number}: dead token removed from registry`);
        }
        console.error(`CALL ${from} -> ${number}: push failed`, detail);
        return this.send(ws, { type: 'call-failed', to, reason: detail });
      }
    }

    // --- caller hung up: stop a ringing invite before the user answers ----
    if (msg.type === 'call-cancel') {
      const attempt = String(msg.attempt || '').trim();
      const to = String(msg.to || msg.room || '').trim();
      await this.loadRegistry();
      let target = attempt ? this.pending.get(attempt) : null;
      if (!target && to) {
        const r = this.resolveEntry(to);
        if (r.entry) target = { to: r.number, expiresAt: 0 };
      }
      if (!target) return;
      this.pending.delete(attempt);
      const entry = this.entryFor(target.to);
      if (entry && entry.token && this.env.FCM_SA) {
        this.pushCallCancel(entry.token, attempt).catch((e) => {
          console.error('CALL-CANCEL push failed', String(e && e.message ? e.message : e));
        });
      }
      // Also reach an app already connected to that mailbox room (it is on the
      // screen and ringing there), then clean up the socket's room state.
      this.broadcast(target.to, ws, { type: 'call-cancel', attempt });
      return;
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
      const existingPeers = Array.from(peers);
      peers.add(ws);
      // Add the new socket BEFORE notifying peers. This removes the E2EE
      // handshake race where the first peer could send its public key after
      // receiving peer-joined but before the new socket had entered the room.
      this.send(ws, { type: 'joined', peers: existingPeers.length });
      for (const peer of existingPeers) this.send(peer, { type: 'peer-joined' });
      // Give the joining peer the existing peers' keys/signals through normal
      // room traffic; each existing peer re-announces its E2EE key on peer-joined.
      return;
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

  // Shared FCM HTTP v1 sender: one message envelope, cached access token.
  async sendMessage(message) {
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
        body: JSON.stringify({ message }),
      }
    );
    if (!res.ok) throw new Error(`FCM send failed: ${res.status} ${await res.text()}`);
    return true;
  }

  async pushCallInvite(token, from, to, fromName, invite = {}) {
    const now = Date.now();
    const expiresAt = Number(invite.expiresAt) || now + INVITE_MAX_LIFETIME_MS;
    const ttlSeconds = Math.max(5, Math.round((expiresAt - now) / 1000));
    await this.sendMessage({
      token,
      // "from"/"to" are reserved FCM envelope keys: putting them inside
      // data makes messages:send reject the payload with 400
      // INVALID_ARGUMENT "Invalid data payload key: from".
      data: {
        type: 'call-invite',
        caller: String(from || ''),
        callee: String(to || ''),
        name: String(fromName || ''),
        ts: String(now),
        expires: String(expiresAt),
        attempt: String(invite.attempt || ''),
      },
      // ttl == remaining lifetime, so FCM itself discards an invite that would
      // land after the caller already gave up.
      android: { priority: 'high', ttl: `${ttlSeconds}s` },
    });
    return true;
  }

  async pushCallCancel(token, attempt) {
    await this.sendMessage({
      token,
      data: {
        type: 'call-cancel',
        attempt: String(attempt || ''),
        ts: String(Date.now()),
      },
      android: { priority: 'high', ttl: '30s' },
    });
    return true;
  }
}
