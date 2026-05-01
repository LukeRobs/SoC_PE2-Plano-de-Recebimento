// Vercel Serverless Function — /api/data (Plano de Recebimento)
// Variáveis de ambiente necessárias no Vercel:
//   GOOGLE_SERVICE_ACCOUNT  — JSON da service account em base64
//   SHEETS_API_KEY          — API Key (planilhas públicas)
//   SPREADSHEET_ID          — ID da planilha de recebimento
//   SHEET_RANGE             — Range (padrão: Sheet1!A1:P3000)

const crypto         = require('crypto');
const SPREADSHEET_ID = process.env.SPREADSHEET_ID || '1jVYf9tecelQtvTGumkWb002O_xjkADOPAUAZ5xy9MEQ';
const RANGE          = process.env.SHEET_RANGE    || 'Daily!A1:Q3000';

// ─── MAPEAMENTO DE COLUNAS ────────────────────────────
const COL = {
  DATE_SOC:       16,  // Q — date_soc           (DD/MM/YY)
  LT:              1,  // B — lh_trip
  ETA_PLAN:        2,  // C — eta_panej
  STATUS:         10,  // K — Status_Real
  TURNO_ETA:       6,  // G — turno_eta           (turno planejado)
  TURNO_DESC:     15,  // P — turno_Descarga      (turno real da descarga)
  HORARIO_DESC:    9,  // J — horario_de_descarga (preenchido = descarregado)
  DESTINO:         5,  // F — origem
  PACOTES:        11,  // L — total_orders
  SACAS:          12,  // M — to_saca
  SCUTTLE:        13,  // N — to_scuttle
  PALLET:         14,  // O — to_pallet
};
// ─────────────────────────────────────────────────────

// ── Service Account JWT ───────────────────────────────
let saToken = null, saTokenExp = 0;

function b64url(buf) {
  return buf.toString('base64').replace(/=/g,'').replace(/\+/g,'-').replace(/\//g,'_');
}

async function getServiceAccountToken(sa) {
  if (saToken && Date.now() < saTokenExp) return saToken;
  const { client_email, private_key } = sa;
  const now = Math.floor(Date.now() / 1000);
  const hdr = b64url(Buffer.from(JSON.stringify({ alg:'RS256', typ:'JWT' })));
  const pay = b64url(Buffer.from(JSON.stringify({
    iss: client_email,
    scope: 'https://www.googleapis.com/auth/spreadsheets',
    aud:  'https://oauth2.googleapis.com/token',
    exp: now + 3600, iat: now,
  })));
  const sign = crypto.createSign('RSA-SHA256');
  sign.update(`${hdr}.${pay}`);
  const jwt = `${hdr}.${pay}.${b64url(sign.sign(private_key))}`;
  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`,
  });
  if (!resp.ok) throw new Error(`Token error: ${resp.status}`);
  const data  = await resp.json();
  saToken     = data.access_token;
  saTokenExp  = Date.now() + (data.expires_in - 60) * 1000;
  return saToken;
}

// ── Helpers ───────────────────────────────────────────

// Converte "DD/MM/YY" → "YYYY-MM-DD"
function parseDateSoc(s) {
  if (!s) return '';
  s = s.trim();
  const parts = s.split('/');
  if (parts.length === 3) {
    const [d, m, y] = parts;
    const year = y.length === 2 ? '20' + y : y;
    return `${year}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
  }
  return s.substring(0, 10);
}

// Normaliza "DD/MM/YYYY HH:MM:SS" ou "YYYY-MM-DD HH:MM:SS" → ISO
function normalizeStr(s) {
  if (!s || s.trim() === '' || s === '.0') return null;
  const str = s.trim();
  if (str.includes('/')) {
    const [datePart, timePart = '00:00:00'] = str.split(' ');
    const parts = datePart.split('/');
    let iso;
    if (parts[2] && parts[2].length === 4) {
      // DD/MM/YYYY
      const [d, m, y] = parts;
      iso = `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
    } else {
      // MM/DD/YYYY (legado)
      const [m, d, y] = parts;
      iso = `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
    }
    const [hh, mm, ss = '00'] = timePart.split(':');
    return `${iso}T${hh.padStart(2,'0')}:${mm}:${ss}`;
  }
  const [datePart, timePart = '00:00:00'] = str.split(' ');
  const [hh, mm, ss = '00'] = timePart.split(':');
  return `${datePart}T${hh.padStart(2,'0')}:${mm}:${ss}`;
}

function extractTime(s) {
  const n = normalizeStr(s);
  return n ? n.substring(11, 16) : '';
}

function parseNum(s) {
  if (!s || s === '.0' || s === '0.0' || s === '0') return 0;
  return Math.round(parseFloat(String(s).trim().replace(/\./g,'').replace(',','.')) || 0);
}

// ── Data Processing ───────────────────────────────────
function processRawData(raw) {
  const rows    = Array.isArray(raw.values) ? raw.values.slice(1) : [];
  const byDate  = {};
  const allRows = [];

  rows.forEach(r => {
    const dateSoc = parseDateSoc(r[COL.DATE_SOC] || '');
    if (!dateSoc || dateSoc.length < 10) return;

    const turno = r[COL.TURNO_DESC] || r[COL.TURNO_ETA] || '';
    if (!turno) return;

    const sr   = r[COL.STATUS]  || '';
    const dest = r[COL.DESTINO] || '';
    const ep   = extractTime(r[COL.ETA_PLAN]);
    const pkg  = parseNum(r[COL.PACOTES]);
    const sac  = parseNum(r[COL.SACAS]);
    const sct  = parseNum(r[COL.SCUTTLE]);
    const plt  = parseNum(r[COL.PALLET]);
    const hdesc = (r[COL.HORARIO_DESC] || '').trim();
    const desc  = hdesc !== '' && hdesc !== '.0';

    allRows.push({
      d: dateSoc,
      lt: r[COL.LT] || '',
      vt: '',
      ep, sr, tr: turno, dest, doca: '',
      pkg, sac, sct, plt, desc: desc ? 1 : 0,
    });

    if (!byDate[dateSoc])        byDate[dateSoc] = {};
    if (!byDate[dateSoc][turno]) byDate[dateSoc][turno] = {
      total:0, desc:0, pkg:0, sac:0, sct:0, plt:0,
      statusCounts:{}, destinos:{}, docas:{},
    };

    const tg = byDate[dateSoc][turno];
    tg.total++;
    tg.pkg += pkg; tg.sac += sac; tg.sct += sct; tg.plt += plt;
    if (desc) tg.desc++;
    tg.statusCounts[sr] = (tg.statusCounts[sr] || 0) + 1;
    if (dest) tg.destinos[dest] = (tg.destinos[dest] || 0) + 1;
  });

  const dates = Object.keys(byDate).sort();
  return { DATES: dates, BY_DATE: byDate, ALL_ROWS: allRows,
           generatedAt: Date.now(), rowCount: allRows.length };
}

// ── Handler ───────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 's-maxage=60, stale-while-revalidate=120');

  try {
    let sheetsUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(RANGE)}`;
    let headers   = {};

    if (process.env.GOOGLE_SERVICE_ACCOUNT) {
      const sa    = JSON.parse(Buffer.from(process.env.GOOGLE_SERVICE_ACCOUNT, 'base64').toString('utf8'));
      const token = await getServiceAccountToken(sa);
      headers     = { Authorization: `Bearer ${token}` };
    } else if (process.env.SHEETS_API_KEY) {
      sheetsUrl += `?key=${process.env.SHEETS_API_KEY}`;
    } else {
      return res.status(500).json({ error: 'Configure GOOGLE_SERVICE_ACCOUNT ou SHEETS_API_KEY.' });
    }

    const response = await fetch(sheetsUrl, { headers });
    if (!response.ok) {
      const body = await response.text();
      throw new Error(`Sheets API ${response.status}: ${body.substring(0, 300)}`);
    }
    const raw  = await response.json();

    if (req.query && req.query.debug === '1') {
      const rows = Array.isArray(raw.values) ? raw.values : [];
      return res.json({
        headers:  rows[0]  || [],
        sample:   rows.slice(1, 4),
        totalRows: rows.length - 1,
      });
    }

    const data = processRawData(raw);
    res.json(data);
  } catch (err) {
    console.error('[api/data] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
};
