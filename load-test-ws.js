import ws from 'k6/ws';
import { check } from 'k6';
import { Trend, Counter, Rate } from 'k6/metrics';

// Prueba de carga WebSocket para websocket-electric.
// Cada VU abre una conexión, hace ping/pong a nivel de aplicación y mide latencia.
//
// Uso:
//   k6 run -e WS_URL=ws://127.0.0.1:39217/ws/connect -e TOKEN=<jwt> load-test-ws.js

const WS_URL = __ENV.WS_URL || 'ws://127.0.0.1:39217/ws/connect';
const TOKEN = __ENV.TOKEN || '';

const pingLatency = new Trend('ws_ping_latency_ms', true);
const pongsRecibidos = new Counter('ws_pongs_recibidos');
const conexionesOK = new Rate('ws_conexiones_ok');

export const options = {
  scenarios: {
    conexiones_sostenidas: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 100 }, // subir a 100 conexiones
        { duration: '15s', target: 250 }, // pico de 250 (piloto)
        { duration: '15s', target: 250 }, // sostener
        { duration: '5s', target: 0 },    // bajar
      ],
    },
  },
  thresholds: {
    ws_conexiones_ok: ['rate>0.99'],
    ws_ping_latency_ms: ['p(95)<500'],
  },
};

export default function () {
  const url = `${WS_URL}?token=${TOKEN}`;
  let lastPing = 0;
  const res = ws.connect(url, {}, function (socket) {
    socket.on('open', function () {
      conexionesOK.add(1);
      // Enviar un ping de aplicación cada 3s y medir el pong.
      socket.setInterval(function () {
        lastPing = Date.now();
        socket.send(JSON.stringify({ type: 'ping' }));
      }, 3000);
    });

    socket.on('message', function (msg) {
      try {
        const m = JSON.parse(msg);
        if (m.type === 'pong' && lastPing) {
          pingLatency.add(Date.now() - lastPing);
          pongsRecibidos.add(1);
        }
      } catch (e) {}
    });

    socket.on('error', function () {
      conexionesOK.add(0);
    });

    // Mantener la conexión abierta ~20s por VU.
    socket.setTimeout(function () {
      socket.close();
    }, 20000);
  });

  check(res, { 'handshake status 101': (r) => r && r.status === 101 });
}
