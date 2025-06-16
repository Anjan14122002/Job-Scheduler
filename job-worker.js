const { parentPort, workerData } = require('worker_threads');
console.log(`[${new Date().toLocaleString()}] Hello World (job #${workerData.id})`);
parentPort.postMessage({ done: true });
