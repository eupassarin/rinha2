import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
    stages: [
        { duration: '10s', target: 50 },
        { duration: '10s', target: 100 },
        { duration: '10s', target: 200 },
        { duration: '10s', target: 0 } //
    ],
    thresholds: {
        'http_req_duration': ['p(95)<1']
    }
};

export default function () {
    // Realiza a solicitação GET
    let res = http.get('http://localhost:9999/clientes/1/extrato');
    check(res, {
        'status is 200': (r) => r.status === 200
    });
    sleep(1);
}