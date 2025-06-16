const express = require('express');
const bodyParser = require('body-parser');
const { Worker } = require('worker_threads');
const app = express();
const PORT = 3000;

const jobs = [];
let nextJobId = 1;

app.use(bodyParser.json());

function runJobInWorker(jobId) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(__dirname + '/job-worker.js', {
      workerData: { id: jobId }
    });
    worker.once('message', msg => msg.done ? resolve() : reject(new Error('Worker failed')));
    worker.once('error', reject);
    worker.once('exit', code => {
      if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
    });
  });
}

function getCurrentMinuteKey() {
  const now = new Date();
  return now.toISOString().slice(0, 16);
}

function startSchedulerLoop() {
  const now = new Date();
  const msUntilNextMinute = (60 - now.getSeconds()) * 1000 - now.getMilliseconds();
  setTimeout(() => {
    checkAllJobsAndRun();
    setInterval(checkAllJobsAndRun, 60000);
  }, msUntilNextMinute);
}

function checkAllJobsAndRun() {
  const now = new Date();
  const curMinute = now.getMinutes();
  const curHour = now.getHours();
  const curDayOfWeek = now.getDay();
  const minuteKey = getCurrentMinuteKey();

  jobs.forEach(job => {
    if (job.lastRun === minuteKey) return;

    let shouldRun = false;
    switch (job.type) {
      case 'hourly':
        if (curMinute === job.minute) shouldRun = true;
        break;
      case 'daily':
        if (curHour === job.hour && curMinute === job.minute) shouldRun = true;
        break;
      case 'weekly':
        if (
          curDayOfWeek === job.dayOfWeek &&
          curHour === job.hour &&
          curMinute === job.minute
        ) {
          shouldRun = true;
        }
        break;
    }

    if (shouldRun) {
      job.lastRun = minuteKey;
      runJobInWorker(job.id)
        .catch(err => console.error(`Job #${job.id} failed:`, err));
    }
  });
}

startSchedulerLoop();

app.get('/jobs', (req, res) => {
  res.json(jobs);
});

app.post('/jobs', (req, res) => {
  const { type, minute, hour, dayOfWeek } = req.body;
  if (!type || !['hourly', 'daily', 'weekly'].includes(type)) {
    return res.status(400).json({ error: 'type must be "hourly", "daily", or "weekly"' });
  }
  if (type === 'hourly') {
    if (typeof minute !== 'number' || minute < 0 || minute > 59) {
      return res.status(400).json({ error: 'For hourly jobs, minute must be 0–59' });
    }
  }
  if (type === 'daily') {
    if (
      typeof hour !== 'number' ||
      hour < 0 ||
      hour > 23 ||
      typeof minute !== 'number' ||
      minute < 0 ||
      minute > 59
    ) {
      return res
        .status(400)
        .json({ error: 'For daily jobs, hour 0–23 and minute 0–59 are required' });
    }
  }
  if (type === 'weekly') {
    if (
      typeof dayOfWeek !== 'number' ||
      dayOfWeek < 0 ||
      dayOfWeek > 6 ||
      typeof hour !== 'number' ||
      hour < 0 ||
      hour > 23 ||
      typeof minute !== 'number' ||
      minute < 0 ||
      minute > 59
    ) {
      return res.status(400).json({
        error: 'For weekly jobs, dayOfWeek 0–6 (0=Sunday), hour 0–23, minute 0–59 are required',
      });
    }
  }

  const newJob = {
    id: nextJobId++,
    type: type,
    minute: minute,
    hour: (type === 'daily' || type === 'weekly') ? hour : undefined,
    dayOfWeek: (type === 'weekly') ? dayOfWeek : undefined,
    lastRun: null
  };

  jobs.push(newJob);
  return res.status(201).json(newJob);
});

app.delete('/jobs/:id', (req, res) => {
  const id = parseInt(req.params.id, 10);
  const idx = jobs.findIndex(j => j.id === id);
  if (idx === -1) return res.status(404).json({ error: 'Job not found' });
  const removed = jobs.splice(idx, 1)[0];
  return res.json({ deleted: removed });
});

app.listen(PORT, () => {
  console.log(`Scheduler API listening on http://localhost:${PORT}`);
});
