import http from 'k6/http';
import {check, sleep} from 'k6';

export const options = {
    stages: [
        {duration: '30s', target: 10},   // aquecimento: sobe gradualmente até 10 usuários virtuais
        {duration: '1m', target: 50},    // aumenta a carga até 50 usuários virtuais
        {duration: '2m', target: 50},    // mantém carga alta por 2 minutos (aqui o HPA deve reagir)
        {duration: '30s', target: 0},    // desce a carga a zero (observar o scale-down depois)
    ],
    thresholds: {
        http_req_failed: ['rate<0.05'],     // menos de 5% de falhas é aceitável
        http_req_duration: ['p(95)<2000'],  // 95% das requisições abaixo de 2s
    },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
    // 80% das vezes: lista as tarefas (leitura)
    if (Math.random() < 0.8) {
        const res = http.get(`${BASE_URL}/api/tasks`);
        check(res, {'GET status é 200': (r) => r.status === 200});
    } else {
        // 20% das vezes: cria uma tarefa nova (escrita, passando pelo Kafka também)
        const payload = JSON.stringify({title: `Tarefa de carga ${Date.now()}`});
        const params = {headers: {'Content-Type': 'application/json'}};
        const res = http.post(`${BASE_URL}/api/tasks`, payload, params);
        check(res, {'POST status é 200': (r) => r.status === 200});
    }

    sleep(0.1);
}