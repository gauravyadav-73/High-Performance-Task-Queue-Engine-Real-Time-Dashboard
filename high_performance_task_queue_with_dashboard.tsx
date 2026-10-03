import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import {
  Play, Pause, RefreshCw, Trash2, ShieldAlert, Cpu, Activity,
  CheckCircle2, XCircle, Clock, AlertTriangle, FastForward,
  Plus, Layers, Terminal, Zap, Filter, Search, BarChart3,
  List, Settings, ChevronRight, ArrowUpRight, ArrowDownRight,
  TrendingUp, PlayCircle, StopCircle, RotateCcw, X, Info
} from 'lucide-react';

const JOB_TYPES = {
  EMAIL: { id: 'email', name: 'Email Dispatch', defaultDuration: 1200, failureRate: 0.1, icon: '📧', color: 'bg-blue-500/10 text-blue-500 border-blue-500/20' },
  IMAGE_PROC: { id: 'image_proc', name: 'Image Processing', defaultDuration: 2500, failureRate: 0.2, icon: '🖼️', color: 'bg-purple-500/10 text-purple-500 border-purple-500/20' },
  DATA_INGEST: { id: 'data_ingest', name: 'Data Ingestion', defaultDuration: 3000, failureRate: 0.15, icon: '💾', color: 'bg-emerald-500/10 text-emerald-500 border-emerald-500/20' },
  WEBHOOK: { id: 'webhook', name: 'Webhook Delivery', defaultDuration: 800, failureRate: 0.05, icon: '🔗', color: 'bg-amber-500/10 text-amber-500 border-amber-500/20' },
  AI_GEN: { id: 'ai_gen', name: 'AI Text Generation', defaultDuration: 4000, failureRate: 0.25, icon: '🤖', color: 'bg-rose-500/10 text-rose-500 border-rose-500/20' },
};

const PRIORITIES = {
  HIGH: { id: 1, label: 'High', color: 'bg-red-500/20 text-red-400 border-red-500/30' },
  MEDIUM: { id: 2, label: 'Medium', color: 'bg-amber-500/20 text-amber-400 border-amber-500/30' },
  LOW: { id: 3, label: 'Low', color: 'bg-blue-500/20 text-blue-400 border-blue-500/30' },
};

const STATUS_COLORS = {
  queued: 'bg-slate-500/20 text-slate-300 border-slate-500/30',
  processing: 'bg-amber-500/20 text-amber-300 border-amber-500/40 animate-pulse',
  completed: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30',
  failed: 'bg-rose-500/20 text-rose-300 border-rose-500/30',
  retrying: 'bg-purple-500/20 text-purple-300 border-purple-500/30',
  delayed: 'bg-sky-500/20 text-sky-300 border-sky-500/30',
};

class QueueEngine {
  constructor() {
    this.jobs = new Map();
    this.concurrency = 4;
    this.isPaused = false;
    this.activeWorkers = 0;
    this.listeners = new Map();
    this.metricsHistory = [];
    this.completedCount = 0;
    this.failedCount = 0;
    this.totalLatency = 0;
    this.processedLastSec = 0;
    this.throughputRate = 0;

    // Start tick loops
    this.startLoop();
    this.startMetricsLoop();
  }

  on(event, callback) {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event).add(callback);
    return () => this.listeners.get(event).delete(callback);
  }

  emit(event, data) {
    if (this.listeners.has(event)) {
      this.listeners.get(event).forEach(cb => cb(data));
    }
  }

  addJob({ type, payload, priority = 2, delayMs = 0, maxRetries = 3 }) {
    const id = `job_${Math.random().toString(36).substring(2, 9)}`;
    const now = Date.now();
    
    const job = {
      id,
      type,
      payload,
      priority,
      status: delayMs > 0 ? 'delayed' : 'queued',
      progress: 0,
      attempts: 0,
      maxRetries,
      createdAt: now,
      delayUntil: delayMs > 0 ? now + delayMs : null,
      startedAt: null,
      completedAt: null,
      failedAt: null,
      error: null,
      logs: [`[${new Date().toLocaleTimeString()}] Job enqueued into queue with priority ${priority}`],
    };

    this.jobs.set(id, job);
    this.emit('job:added', job);
    this.emit('state:change', this.getState());
    return job;
  }

  addBatch(count = 50, jobTypeKey = 'EMAIL') {
    const types = Object.keys(JOB_TYPES);
    for (let i = 0; i < count; i++) {
      const selectedType = jobTypeKey === 'RANDOM' 
        ? types[Math.floor(Math.random() * types.length)]
        : jobTypeKey;
      const typeConfig = JOB_TYPES[selectedType] || JOB_TYPES.EMAIL;
      
      this.addJob({
        type: selectedType,
        payload: { batchBatchIndex: i + 1, target: `user_${i + 1}@system.io`, payloadSizeKb: Math.floor(Math.random() * 500) },
        priority: Math.floor(Math.random() * 3) + 1,
        delayMs: Math.random() < 0.2 ? Math.floor(Math.random() * 3000) : 0,
        maxRetries: 3
      });
    }
  }

  startLoop() {
    setInterval(() => {
      if (this.isPaused) return;

      const now = Date.now();
      // Check delayed jobs
      for (const job of this.jobs.values()) {
        if (job.status === 'delayed' && job.delayUntil && now >= job.delayUntil) {
          job.status = 'queued';
          job.logs.push(`[${new Date().toLocaleTimeString()}] Delay period elapsed. Moved to queued state.`);
          this.emit('job:updated', job);
        }
      }

      // Process queued jobs up to concurrency capacity
      while (this.activeWorkers < this.concurrency) {
        const nextJob = this.getNextJob();
        if (!nextJob) break;
        this.processJob(nextJob);
      }
    }, 50);
  }

  getNextJob() {
    let bestJob = null;
    for (const job of this.jobs.values()) {
      if (job.status !== 'queued') continue;
      if (!bestJob) {
        bestJob = job;
      } else {
        // Priority sort (1 high, 3 low) -> then FIFO
        if (job.priority < bestJob.priority) {
          bestJob = job;
        } else if (job.priority === bestJob.priority && job.createdAt < bestJob.createdAt) {
          bestJob = job;
        }
      }
    }
    return bestJob;
  }

  async processJob(job) {
    this.activeWorkers++;
    job.status = 'processing';
    job.startedAt = Date.now();
    job.attempts++;
    job.logs.push(`[${new Date().toLocaleTimeString()}] Processing start (Attempt ${job.attempts}/${job.maxRetries + 1})`);
    this.emit('job:updated', job);
    this.emit('state:change', this.getState());

    const typeConfig = JOB_TYPES[job.type] || JOB_TYPES.EMAIL;
    const duration = typeConfig.defaultDuration * (0.6 + Math.random() * 0.8);
    const steps = 10;
    const stepDuration = duration / steps;

    let failed = false;

    for (let i = 1; i <= steps; i++) {
      await new Promise(r => setTimeout(r, stepDuration));
      if (this.isPaused) {
        // Queue paused while running
      }
      
      // Simulate random step failure based on failure rate config
      if (i === Math.floor(steps / 2) && Math.random() < typeConfig.failureRate) {
        failed = true;
        break;
      }

      job.progress = Math.min(100, Math.floor((i / steps) * 100));
      this.emit('job:progress', { id: job.id, progress: job.progress });
      this.emit('state:change', this.getState());
    }

    if (failed) {
      job.failedAt = Date.now();
      const errMessage = `Unhandled execution error in task engine at step module ${Math.floor(Math.random() * 4 + 1)}`;
      job.error = errMessage;
      job.logs.push(`[${new Date().toLocaleTimeString()}] ERROR: ${errMessage}`);

      if (job.attempts <= job.maxRetries) {
        job.status = 'retrying';
        const backoffMs = Math.pow(2, job.attempts) * 1000 + Math.random() * 500; // Exponential Backoff + Jitter
        job.delayUntil = Date.now() + backoffMs;
        job.logs.push(`[${new Date().toLocaleTimeString()}] Retrying job in ${(backoffMs / 1000).toFixed(1)}s (Backoff Applied)`);
        
        setTimeout(() => {
          if (job.status === 'retrying') {
            job.status = 'queued';
            this.emit('job:updated', job);
            this.emit('state:change', this.getState());
          }
        }, backoffMs);
      } else {
        job.status = 'failed';
        this.failedCount++;
        this.emit('job:failed', job);
      }
    } else {
      job.status = 'completed';
      job.progress = 100;
      job.completedAt = Date.now();
      const runTime = job.completedAt - job.startedAt;
      this.totalLatency += runTime;
      this.completedCount++;
      this.processedLastSec++;
      job.logs.push(`[${new Date().toLocaleTimeString()}] Job completed successfully in ${runTime}ms`);
      this.emit('job:completed', job);
    }

    this.activeWorkers--;
    this.emit('job:updated', job);
    this.emit('state:change', this.getState());
  }

  startMetricsLoop() {
    setInterval(() => {
      this.throughputRate = this.processedLastSec;
      this.processedLastSec = 0;

      const state = this.getState();
      const historyPoint = {
        time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
        throughput: this.throughputRate,
        queued: state.counts.queued,
        processing: state.counts.processing,
        activeWorkers: this.activeWorkers,
      };

      this.metricsHistory = [...this.metricsHistory.slice(-20), historyPoint];
      this.emit('metrics:update', this.metricsHistory);
    }, 1000);
  }

  retryJob(id) {
    const job = this.jobs.get(id);
    if (job && (job.status === 'failed' || job.status === 'completed')) {
      job.status = 'queued';
      job.progress = 0;
      job.error = null;
      job.logs.push(`[${new Date().toLocaleTimeString()}] Manual retry triggered by operator`);
      this.emit('job:updated', job);
      this.emit('state:change', this.getState());
    }
  }

  cancelJob(id) {
    const job = this.jobs.get(id);
    if (job && (job.status === 'queued' || job.status === 'delayed')) {
      this.jobs.delete(id);
      this.emit('state:change', this.getState());
    }
  }

  deleteJob(id) {
    this.jobs.delete(id);
    this.emit('state:change', this.getState());
  }

  clearByStatus(status) {
    for (const [id, job] of this.jobs.entries()) {
      if (job.status === status) {
        this.jobs.delete(id);
      }
    }
    this.emit('state:change', this.getState());
  }

  setConcurrency(val) {
    this.concurrency = val;
    this.emit('state:change', this.getState());
  }

  togglePause() {
    this.isPaused = !this.isPaused;
    this.emit('state:change', this.getState());
  }

  getState() {
    const jobsList = Array.from(this.jobs.values());
    const counts = {
      all: jobsList.length,
      queued: 0,
      processing: 0,
      completed: 0,
      failed: 0,
      retrying: 0,
      delayed: 0,
    };

    jobsList.forEach(j => {
      if (counts[j.status] !== undefined) counts[j.status]++;
    });

    const totalProcessed = this.completedCount + this.failedCount;
    const avgLatency = this.completedCount > 0 ? Math.round(this.totalLatency / this.completedCount) : 0;

    return {
      jobs: jobsList,
      counts,
      concurrency: this.concurrency,
      activeWorkers: this.activeWorkers,
      isPaused: this.isPaused,
      avgLatency,
      throughput: this.throughputRate,
      completedCount: this.completedCount,
      failedCount: this.failedCount,
    };
  }
}

// Global Singleton Instance for Demo
const queueEngine = new QueueEngine();

export default function App() {
  const [engineState, setEngineState] = useState(() => queueEngine.getState());
  const [metricsHistory, setMetricsHistory] = useState([]);
  const [logs, setLogs] = useState([]);
  const [selectedJob, setSelectedJob] = useState(null);
  const [statusFilter, setStatusFilter] = useState('all');
  const [searchQuery, setSearchQuery] = useState('');
  const [activeTab, setActiveTab] = useState('dashboard'); // dashboard | jobs | produce | logs

  // Job Submission Form State
  const [formJobType, setFormJobType] = useState('EMAIL');
  const [formPriority, setFormPriority] = useState('2');
  const [formDelay, setFormDelay] = useState(0);
  const [formPayload, setFormPayload] = useState('{\n  "recipient": "user@example.com",\n  "template": "welcome_onboarding"\n}');

  useEffect(() => {
    const unSubState = queueEngine.on('state:change', (newState) => {
      setEngineState({ ...newState });
    });

    const unSubMetrics = queueEngine.on('metrics:update', (history) => {
      setMetricsHistory([...history]);
    });

    const addLogEntry = (msg, level = 'info') => {
      const entry = { id: Math.random().toString(), time: new Date().toLocaleTimeString(), msg, level };
      setLogs(prev => [entry, ...prev.slice(0, 100)]);
    };

    const unSubAdded = queueEngine.on('job:added', (j) => addLogEntry(`[Enqueued] Job #${j.id} (${j.type}) - Priority: ${j.priority}`, 'info'));
    const unSubComp = queueEngine.on('job:completed', (j) => addLogEntry(`[Completed] Job #${j.id} finished successfully`, 'success'));
    const unSubFail = queueEngine.on('job:failed', (j) => addLogEntry(`[Failed] Job #${j.id} failed: ${j.error}`, 'error'));

    // Populate initial sample seed batch
    queueEngine.addBatch(8, 'RANDOM');

    return () => {
      unSubState();
      unSubMetrics();
      unSubAdded();
      unSubComp();
      unSubFail();
    };
  }, []);

  const handleDispatchCustomJob = (e) => {
    e.preventDefault();
    try {
      const parsed = JSON.parse(formPayload);
      queueEngine.addJob({
        type: formJobType,
        payload: parsed,
        priority: parseInt(formPriority, 10),
        delayMs: parseInt(formDelay, 10),
      });
    } catch (err) {
      alert("Invalid JSON Payload format!");
    }
  };

  const filteredJobs = useMemo(() => {
    return engineState.jobs.filter(job => {
      const matchesFilter = statusFilter === 'all' || job.status === statusFilter;
      const matchesSearch = job.id.toLowerCase().includes(searchQuery.toLowerCase()) || 
                            job.type.toLowerCase().includes(searchQuery.toLowerCase());
      return matchesFilter && matchesSearch;
    }).sort((a, b) => b.createdAt - a.createdAt);
  }, [engineState.jobs, statusFilter, searchQuery]);

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 font-sans flex flex-col md:flex-row antialiased selection:bg-indigo-500/30">
      
      {}
      <aside className="w-full md:w-64 bg-slate-900/80 border-r border-slate-800/80 p-4 flex flex-col justify-between backdrop-blur-md">
        <div>
          {/* Logo / Header */}
          <div className="flex items-center gap-3 px-2 py-3 mb-6 border-b border-slate-800">
            <div className="bg-gradient-to-tr from-indigo-500 to-purple-500 p-2 rounded-xl shadow-lg shadow-indigo-500/20">
              <Cpu className="w-6 h-6 text-white" />
            </div>
            <div>
              <h1 className="font-bold text-base tracking-tight bg-gradient-to-r from-white to-slate-400 bg-clip-text text-transparent">
                TaskPulse HQ
              </h1>
              <div className="flex items-center gap-1.5 text-xs text-emerald-400 font-mono">
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
                </span>
                Engine Online
              </div>
            </div>
          </div>

          {/* Navigation Links */}
          <nav className="space-y-1">
            <button
              onClick={() => setActiveTab('dashboard')}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all ${
                activeTab === 'dashboard' 
                  ? 'bg-indigo-600/20 text-indigo-300 border border-indigo-500/30 shadow-sm' 
                  : 'text-slate-400 hover:bg-slate-800/50 hover:text-slate-200'
              }`}
            >
              <BarChart3 className="w-4 h-4" />
              Overview & Analytics
            </button>

            <button
              onClick={() => setActiveTab('jobs')}
              className={`w-full flex items-center justify-between px-3 py-2.5 rounded-lg text-sm font-medium transition-all ${
                activeTab === 'jobs' 
                  ? 'bg-indigo-600/20 text-indigo-300 border border-indigo-500/30 shadow-sm' 
                  : 'text-slate-400 hover:bg-slate-800/50 hover:text-slate-200'
              }`}
            >
              <div className="flex items-center gap-3">
                <List className="w-4 h-4" />
                Job Inspector
              </div>
              <span className="bg-slate-800 text-slate-300 text-xs px-2 py-0.5 rounded-full border border-slate-700">
                {engineState.counts.all}
              </span>
            </button>

            <button
              onClick={() => setActiveTab('produce')}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all ${
                activeTab === 'produce' 
                  ? 'bg-indigo-600/20 text-indigo-300 border border-indigo-500/30 shadow-sm' 
                  : 'text-slate-400 hover:bg-slate-800/50 hover:text-slate-200'
              }`}
            >
              <Zap className="w-4 h-4" />
              Producer Sandbox
            </button>

            <button
              onClick={() => setActiveTab('logs')}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all ${
                activeTab === 'logs' 
                  ? 'bg-indigo-600/20 text-indigo-300 border border-indigo-500/30 shadow-sm' 
                  : 'text-slate-400 hover:bg-slate-800/50 hover:text-slate-200'
              }`}
            >
              <Terminal className="w-4 h-4" />
              Live Terminal Log
            </button>
          </nav>
        </div>

        {/* Global Controls in Sidebar Footer */}
        <div className="pt-4 border-t border-slate-800 space-y-4">
          <div className="bg-slate-950/60 p-3 rounded-lg border border-slate-800/80">
            <div className="flex justify-between items-center text-xs mb-2 text-slate-400">
              <span>Worker Concurrency</span>
              <span className="font-mono text-indigo-400 font-bold">{engineState.concurrency} Workers</span>
            </div>
            <input
              type="range"
              min="1"
              max="10"
              value={engineState.concurrency}
              onChange={(e) => queueEngine.setConcurrency(parseInt(e.target.value, 10))}
              className="w-full accent-indigo-500 cursor-pointer h-1.5 bg-slate-800 rounded-lg appearance-none"
            />
          </div>

          <button
            onClick={() => queueEngine.togglePause()}
            className={`w-full flex items-center justify-center gap-2 py-2 px-4 rounded-lg font-medium text-xs border transition-all ${
              engineState.isPaused
                ? 'bg-amber-500/20 border-amber-500/40 text-amber-300 hover:bg-amber-500/30'
                : 'bg-indigo-600/20 border-indigo-500/40 text-indigo-300 hover:bg-indigo-600/30'
            }`}
          >
            {engineState.isPaused ? <Play className="w-3.5 h-3.5 fill-amber-300" /> : <Pause className="w-3.5 h-3.5 fill-indigo-300" />}
            {engineState.isPaused ? 'Resume Processing' : 'Pause Queue Processing'}
          </button>
        </div>
      </aside>

      {}
      <main className="flex-1 overflow-y-auto flex flex-col">
        {/* Top Navbar */}
        <header className="bg-slate-900/40 border-b border-slate-800/80 p-4 sticky top-0 backdrop-blur-md z-10 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <h2 className="text-lg font-semibold text-slate-100 capitalize">{activeTab.replace('_', ' ')} View</h2>
            {engineState.isPaused && (
              <span className="bg-amber-500/10 text-amber-400 border border-amber-500/20 text-xs px-2.5 py-0.5 rounded-full flex items-center gap-1">
                <AlertTriangle className="w-3 h-3" /> Queue Paused
              </span>
            )}
          </div>

          <div className="flex items-center gap-2">
            <button
              onClick={() => queueEngine.addBatch(25, 'RANDOM')}
              className="bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-semibold px-3 py-1.5 rounded-lg shadow-lg shadow-indigo-600/20 transition-all flex items-center gap-1.5"
            >
              <Plus className="w-3.5 h-3.5" /> Spawn +25 Batch
            </button>
            <button
              onClick={() => queueEngine.clearByStatus('completed')}
              className="bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700 text-xs font-medium px-3 py-1.5 rounded-lg transition-all"
            >
              Purge Completed
            </button>
          </div>
        </header>

        {/* Content Views Routing */}
        <div className="p-6 space-y-6 flex-1">

          {}
          {activeTab === 'dashboard' && (
            <div className="space-y-6">
              {/* Executive Stat Cards */}
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <div className="bg-slate-900/60 border border-slate-800 p-4 rounded-xl relative overflow-hidden">
                  <div className="flex justify-between items-start">
                    <div>
                      <p className="text-xs font-medium text-slate-400">Active Workers</p>
                      <h3 className="text-2xl font-bold mt-1 text-slate-100">
                        {engineState.activeWorkers} / <span className="text-sm font-normal text-slate-400">{engineState.concurrency} capacity</span>
                      </h3>
                    </div>
                    <div className="p-2.5 bg-indigo-500/10 text-indigo-400 rounded-lg">
                      <Cpu className="w-5 h-5" />
                    </div>
                  </div>
                  <div className="w-full bg-slate-800 h-1.5 rounded-full mt-4 overflow-hidden">
                    <div 
                      className="bg-indigo-500 h-full transition-all duration-300"
                      style={{ width: `${(engineState.activeWorkers / engineState.concurrency) * 100}%` }}
                    />
                  </div>
                </div>

                <div className="bg-slate-900/60 border border-slate-800 p-4 rounded-xl">
                  <div className="flex justify-between items-start">
                    <div>
                      <p className="text-xs font-medium text-slate-400">Current Queue Depth</p>
                      <h3 className="text-2xl font-bold mt-1 text-slate-100">{engineState.counts.queued}</h3>
                    </div>
                    <div className="p-2.5 bg-amber-500/10 text-amber-400 rounded-lg">
                      <Layers className="w-5 h-5" />
                    </div>
                  </div>
                  <p className="text-xs text-slate-400 mt-3 flex items-center gap-1">
                    <Clock className="w-3.5 h-3.5 text-sky-400" /> {engineState.counts.delayed} delayed jobs pending
                  </p>
                </div>

                <div className="bg-slate-900/60 border border-slate-800 p-4 rounded-xl">
                  <div className="flex justify-between items-start">
                    <div>
                      <p className="text-xs font-medium text-slate-400">Avg Execution Latency</p>
                      <h3 className="text-2xl font-bold mt-1 text-slate-100">{engineState.avgLatency} <span className="text-xs font-normal text-slate-400">ms</span></h3>
                    </div>
                    <div className="p-2.5 bg-emerald-500/10 text-emerald-400 rounded-lg">
                      <Activity className="w-5 h-5" />
                    </div>
                  </div>
                  <p className="text-xs text-slate-400 mt-3 flex items-center gap-1">
                    <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" /> {engineState.completedCount} total succeeded
                  </p>
                </div>

                <div className="bg-slate-900/60 border border-slate-800 p-4 rounded-xl">
                  <div className="flex justify-between items-start">
                    <div>
                      <p className="text-xs font-medium text-slate-400">Throughput Rate</p>
                      <h3 className="text-2xl font-bold mt-1 text-slate-100">{engineState.throughput} <span className="text-xs font-normal text-slate-400">jobs/sec</span></h3>
                    </div>
                    <div className="p-2.5 bg-purple-500/10 text-purple-400 rounded-lg">
                      <TrendingUp className="w-5 h-5" />
                    </div>
                  </div>
                  <p className="text-xs text-slate-400 mt-3 flex items-center gap-1">
                    <XCircle className="w-3.5 h-3.5 text-rose-400" /> {engineState.failedCount} failures recorded
                  </p>
                </div>
              </div>

              {/* Live Charts Section */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Custom Sparkline SVG Chart: Queue Metrics Timeline */}
                <div className="bg-slate-900/60 border border-slate-800 p-5 rounded-xl flex flex-col">
                  <div className="flex justify-between items-center mb-4">
                    <h3 className="font-semibold text-sm text-slate-200">Throughput History (jobs/sec)</h3>
                    <span className="text-xs font-mono bg-indigo-500/10 text-indigo-400 px-2 py-0.5 rounded border border-indigo-500/20">Live 1s Interval</span>
                  </div>

                  <div className="h-48 w-full flex items-end gap-1.5 pt-6 pb-2 border-b border-slate-800">
                    {metricsHistory.length === 0 ? (
                      <div className="w-full h-full flex items-center justify-center text-xs text-slate-500">
                        Gathering engine metrics payload...
                      </div>
                    ) : (
                      metricsHistory.map((m, idx) => {
                        const maxTp = Math.max(...metricsHistory.map(x => x.throughput), 10);
                        const heightPct = Math.max((m.throughput / maxTp) * 100, 4);
                        return (
                          <div key={idx} className="flex-1 flex flex-col items-center gap-1 group relative">
                            {/* Hover Tooltip */}
                            <div className="absolute -top-8 bg-slate-800 text-slate-200 text-[10px] px-2 py-1 rounded opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none whitespace-nowrap z-20 border border-slate-700">
                              {m.throughput} jobs/s at {m.time}
                            </div>
                            <div
                              className="w-full bg-indigo-500/60 hover:bg-indigo-400 rounded-t transition-all duration-300"
                              style={{ height: `${heightPct}%` }}
                            />
                          </div>
                        );
                      })
                    )}
                  </div>
                  <div className="flex justify-between text-[10px] text-slate-500 mt-2 font-mono">
                    <span>-20 Seconds Ago</span>
                    <span>Now</span>
                  </div>
                </div>

                {/* Queue Depth Realtime Monitor Chart */}
                <div className="bg-slate-900/60 border border-slate-800 p-5 rounded-xl flex flex-col">
                  <div className="flex justify-between items-center mb-4">
                    <h3 className="font-semibold text-sm text-slate-200">Queue Backlog Depth</h3>
                    <span className="text-xs font-mono bg-amber-500/10 text-amber-400 px-2 py-0.5 rounded border border-amber-500/20">Pending Jobs</span>
                  </div>

                  <div className="h-48 w-full flex items-end gap-1.5 pt-6 pb-2 border-b border-slate-800">
                    {metricsHistory.length === 0 ? (
                      <div className="w-full h-full flex items-center justify-center text-xs text-slate-500">
                        Awaiting queue telemetry...
                      </div>
                    ) : (
                      metricsHistory.map((m, idx) => {
                        const maxQueued = Math.max(...metricsHistory.map(x => x.queued), 20);
                        const heightPct = Math.max((m.queued / maxQueued) * 100, 4);
                        return (
                          <div key={idx} className="flex-1 flex flex-col items-center gap-1 group relative">
                            <div className="absolute -top-8 bg-slate-800 text-slate-200 text-[10px] px-2 py-1 rounded opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none whitespace-nowrap z-20 border border-slate-700">
                              {m.queued} queued at {m.time}
                            </div>
                            <div
                              className="w-full bg-amber-500/50 hover:bg-amber-400 rounded-t transition-all duration-300"
                              style={{ height: `${heightPct}%` }}
                            />
                          </div>
                        );
                      })
                    )}
                  </div>
                  <div className="flex justify-between text-[10px] text-slate-500 mt-2 font-mono">
                    <span>-20 Seconds Ago</span>
                    <span>Now</span>
                  </div>
                </div>
              </div>

              {/* Status Breakdowns Grid */}
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
                {[
                  { key: 'queued', label: 'Queued', val: engineState.counts.queued, color: STATUS_COLORS.queued },
                  { key: 'processing', label: 'Processing', val: engineState.counts.processing, color: STATUS_COLORS.processing },
                  { key: 'completed', label: 'Completed', val: engineState.counts.completed, color: STATUS_COLORS.completed },
                  { key: 'failed', label: 'Failed', val: engineState.counts.failed, color: STATUS_COLORS.failed },
                  { key: 'retrying', label: 'Retrying', val: engineState.counts.retrying, color: STATUS_COLORS.retrying },
                  { key: 'delayed', label: 'Delayed', val: engineState.counts.delayed, color: STATUS_COLORS.delayed },
                ].map((st) => (
                  <button
                    key={st.key}
                    onClick={() => { setStatusFilter(st.key); setActiveTab('jobs'); }}
                    className="p-3 bg-slate-900/40 border border-slate-800 hover:border-slate-700 rounded-xl text-left transition-all group"
                  >
                    <span className={`inline-block px-2 py-0.5 text-[10px] uppercase font-bold rounded border ${st.color}`}>
                      {st.label}
                    </span>
                    <p className="text-xl font-bold mt-2 text-slate-100 group-hover:text-indigo-400 transition-colors">{st.val}</p>
                  </button>
                ))}
              </div>
            </div>
          )}

          {}
          {activeTab === 'jobs' && (
            <div className="space-y-4">
              {/* Filter and Search Bar */}
              <div className="flex flex-col sm:flex-row gap-3 justify-between items-center bg-slate-900/60 p-3 rounded-xl border border-slate-800">
                <div className="flex items-center gap-2 w-full sm:w-auto overflow-x-auto pb-1 sm:pb-0">
                  {['all', 'queued', 'processing', 'completed', 'failed', 'retrying', 'delayed'].map(st => (
                    <button
                      key={st}
                      onClick={() => setStatusFilter(st)}
                      className={`px-3 py-1.5 rounded-lg text-xs capitalize font-medium transition-all ${
                        statusFilter === st 
                          ? 'bg-indigo-600 text-white shadow-md shadow-indigo-600/20' 
                          : 'text-slate-400 hover:bg-slate-800 hover:text-slate-200'
                      }`}
                    >
                      {st} ({st === 'all' ? engineState.counts.all : engineState.counts[st] || 0})
                    </button>
                  ))}
                </div>

                <div className="relative w-full sm:w-64">
                  <Search className="w-4 h-4 absolute left-3 top-2.5 text-slate-500" />
                  <input
                    type="text"
                    placeholder="Search Job ID or Type..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="w-full bg-slate-950 border border-slate-800 rounded-lg pl-9 pr-3 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-indigo-500"
                  />
                </div>
              </div>

              {/* Jobs Table */}
              <div className="bg-slate-900/60 border border-slate-800 rounded-xl overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-left text-xs text-slate-300">
                    <thead className="bg-slate-950/80 text-slate-400 border-b border-slate-800 font-mono">
                      <tr>
                        <th className="p-3">Job ID</th>
                        <th className="p-3">Type</th>
                        <th className="p-3">Priority</th>
                        <th className="p-3">Status</th>
                        <th className="p-3">Progress</th>
                        <th className="p-3">Attempts</th>
                        <th className="p-3">Created</th>
                        <th className="p-3 text-right">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/60">
                      {filteredJobs.length === 0 ? (
                        <tr>
                          <td colSpan="8" className="text-center py-8 text-slate-500">
                            No matching jobs found in queue state.
                          </td>
                        </tr>
                      ) : (
                        filteredJobs.map((job) => {
                          const typeConfig = JOB_TYPES[job.type] || JOB_TYPES.EMAIL;
                          const priorityInfo = Object.values(PRIORITIES).find(p => p.id === job.priority) || PRIORITIES.MEDIUM;
                          
                          return (
                            <tr key={job.id} className="hover:bg-slate-800/30 transition-colors">
                              <td className="p-3 font-mono text-indigo-300 font-medium">
                                #{job.id}
                              </td>
                              <td className="p-3">
                                <span className={`inline-flex items-center gap-1.5 px-2 py-1 rounded border font-medium ${typeConfig.color}`}>
                                  <span>{typeConfig.icon}</span>
                                  {typeConfig.name}
                                </span>
                              </td>
                              <td className="p-3">
                                <span className={`px-2 py-0.5 rounded text-[10px] font-semibold border ${priorityInfo.color}`}>
                                  {priorityInfo.label}
                                </span>
                              </td>
                              <td className="p-3">
                                <span className={`px-2.5 py-0.5 rounded-full text-[10px] font-bold border capitalize ${STATUS_COLORS[job.status]}`}>
                                  {job.status}
                                </span>
                              </td>
                              <td className="p-3 w-32">
                                <div className="flex items-center gap-2">
                                  <div className="flex-1 bg-slate-800 h-1.5 rounded-full overflow-hidden">
                                    <div
                                      className="bg-indigo-500 h-full transition-all duration-200"
                                      style={{ width: `${job.progress}%` }}
                                    />
                                  </div>
                                  <span className="font-mono text-[10px] text-slate-400">{job.progress}%</span>
                                </div>
                              </td>
                              <td className="p-3 font-mono text-slate-400">
                                {job.attempts} / {job.maxRetries + 1}
                              </td>
                              <td className="p-3 font-mono text-slate-400">
                                {new Date(job.createdAt).toLocaleTimeString()}
                              </td>
                              <td className="p-3 text-right space-x-1">
                                <button
                                  onClick={() => setSelectedJob(job)}
                                  className="p-1.5 hover:bg-slate-800 text-slate-300 hover:text-white rounded"
                                  title="Inspect Details"
                                >
                                  <Info className="w-4 h-4" />
                                </button>
                                {(job.status === 'failed' || job.status === 'completed') && (
                                  <button
                                    onClick={() => queueEngine.retryJob(job.id)}
                                    className="p-1.5 hover:bg-slate-800 text-indigo-400 hover:text-indigo-300 rounded"
                                    title="Re-enqueue Job"
                                  >
                                    <RotateCcw className="w-4 h-4" />
                                  </button>
                                )}
                                <button
                                  onClick={() => queueEngine.deleteJob(job.id)}
                                  className="p-1.5 hover:bg-slate-800 text-rose-400 hover:text-rose-300 rounded"
                                  title="Delete Job"
                                >
                                  <Trash2 className="w-4 h-4" />
                                </button>
                              </td>
                            </tr>
                          );
                        })
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}

          {}
          {activeTab === 'produce' && (
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              {/* Manual Dispatch Form */}
              <div className="lg:col-span-2 bg-slate-900/60 border border-slate-800 p-5 rounded-xl space-y-4">
                <h3 className="text-base font-semibold text-slate-100 flex items-center gap-2">
                  <Zap className="w-5 h-5 text-indigo-400" /> Dispatch Custom Job
                </h3>

                <form onSubmit={handleDispatchCustomJob} className="space-y-4">
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-xs font-medium text-slate-400 mb-1">Job Type Handler</label>
                      <select
                        value={formJobType}
                        onChange={(e) => setFormJobType(e.target.value)}
                        className="w-full bg-slate-950 border border-slate-800 rounded-lg p-2.5 text-xs text-slate-200 focus:outline-none focus:border-indigo-500"
                      >
                        {Object.entries(JOB_TYPES).map(([key, config]) => (
                          <option key={key} value={key}>{config.icon} {config.name}</option>
                        ))}
                      </select>
                    </div>

                    <div>
                      <label className="block text-xs font-medium text-slate-400 mb-1">Priority Level</label>
                      <select
                        value={formPriority}
                        onChange={(e) => setFormPriority(e.target.value)}
                        className="w-full bg-slate-950 border border-slate-800 rounded-lg p-2.5 text-xs text-slate-200 focus:outline-none focus:border-indigo-500"
                      >
                        <option value="1">High Priority (1)</option>
                        <option value="2">Medium Priority (2)</option>
                        <option value="3">Low Priority (3)</option>
                      </select>
                    </div>
                  </div>

                  <div>
                    <label className="block text-xs font-medium text-slate-400 mb-1">Execution Delay (Milliseconds)</label>
                    <input
                      type="number"
                      value={formDelay}
                      onChange={(e) => setFormDelay(e.target.value)}
                      placeholder="0 for immediate execution"
                      className="w-full bg-slate-950 border border-slate-800 rounded-lg p-2.5 text-xs text-slate-200 focus:outline-none focus:border-indigo-500 font-mono"
                    />
                  </div>

                  <div>
                    <label className="block text-xs font-medium text-slate-400 mb-1">JSON Payload Data</label>
                    <textarea
                      rows="6"
                      value={formPayload}
                      onChange={(e) => setFormPayload(e.target.value)}
                      className="w-full bg-slate-950 border border-slate-800 rounded-lg p-3 text-xs text-indigo-300 font-mono focus:outline-none focus:border-indigo-500"
                    />
                  </div>

                  <button
                    type="submit"
                    className="w-full bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-semibold py-2.5 rounded-lg shadow-lg shadow-indigo-600/20 transition-all flex items-center justify-center gap-2"
                  >
                    <Plus className="w-4 h-4" /> Enqueue Custom Job
                  </button>
                </form>
              </div>

              {/* Load Testing / Stress Sandbox Controls */}
              <div className="bg-slate-900/60 border border-slate-800 p-5 rounded-xl space-y-4">
                <h3 className="text-base font-semibold text-slate-100 flex items-center gap-2">
                  <FastForward className="w-5 h-5 text-amber-400" /> Benchmark Batch Spawner
                </h3>
                <p className="text-xs text-slate-400">
                  Simulate high traffic loads by spawning multiple concurrent tasks into the worker queue.
                </p>

                <div className="space-y-3 pt-2">
                  <button
                    onClick={() => queueEngine.addBatch(50, 'RANDOM')}
                    className="w-full bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-200 text-xs font-medium py-3 rounded-lg transition-all flex items-center justify-between px-4"
                  >
                    <span>Spawn 50 Random Jobs</span>
                    <span className="bg-slate-900 px-2 py-0.5 rounded text-[10px] text-slate-400 border border-slate-800">Batch +50</span>
                  </button>

                  <button
                    onClick={() => queueEngine.addBatch(200, 'RANDOM')}
                    className="w-full bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-200 text-xs font-medium py-3 rounded-lg transition-all flex items-center justify-between px-4"
                  >
                    <span>Spawn 200 Mixed Load Tasks</span>
                    <span className="bg-slate-900 px-2 py-0.5 rounded text-[10px] text-slate-400 border border-slate-800">Batch +200</span>
                  </button>

                  <button
                    onClick={() => queueEngine.addBatch(500, 'AI_GEN')}
                    className="w-full bg-rose-600/20 hover:bg-rose-600/30 border border-rose-500/30 text-rose-300 text-xs font-medium py-3 rounded-lg transition-all flex items-center justify-between px-4"
                  >
                    <span>Stress Test (500 Heavy AI Tasks)</span>
                    <span className="bg-rose-950/60 px-2 py-0.5 rounded text-[10px] text-rose-400 border border-rose-800/50">Stress Test</span>
                  </button>
                </div>
              </div>
            </div>
          )}

          {}
          {activeTab === 'logs' && (
            <div className="bg-slate-950 border border-slate-800 rounded-xl p-4 font-mono text-xs text-slate-300 flex flex-col h-[600px]">
              <div className="flex justify-between items-center pb-3 mb-3 border-b border-slate-800 text-slate-500">
                <span className="flex items-center gap-2">
                  <Terminal className="w-4 h-4 text-emerald-400" /> Event LifeCycle Stream Terminal
                </span>
                <button
                  onClick={() => setLogs([])}
                  className="hover:text-slate-300 text-[10px] uppercase font-bold"
                >
                  Clear Terminal Output
                </button>
              </div>

              <div className="flex-1 overflow-y-auto space-y-1.5 pr-2">
                {logs.length === 0 ? (
                  <div className="text-slate-600 italic">No events logged yet...</div>
                ) : (
                  logs.map((l) => (
                    <div key={l.id} className="flex gap-3">
                      <span className="text-slate-600 select-none">{l.time}</span>
                      <span className={`flex-1 ${
                        l.level === 'error' ? 'text-rose-400' :
                        l.level === 'success' ? 'text-emerald-400' :
                        'text-slate-300'
                      }`}>
                        {l.msg}
                      </span>
                    </div>
                  ))
                )}
              </div>
            </div>
          )}

        </div>
      </main>

      {}
      {selectedJob && (
        <div className="fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-50 flex justify-end">
          <div className="w-full max-w-lg bg-slate-900 border-l border-slate-800 h-full p-6 overflow-y-auto space-y-6 animate-in slide-in-from-right duration-200">
            <div className="flex justify-between items-start border-b border-slate-800 pb-4">
              <div>
                <span className={`px-2.5 py-0.5 rounded-full text-[10px] font-bold border capitalize ${STATUS_COLORS[selectedJob.status]}`}>
                  {selectedJob.status}
                </span>
                <h3 className="text-lg font-bold text-slate-100 font-mono mt-2">
                  #{selectedJob.id}
                </h3>
              </div>
              <button
                onClick={() => setSelectedJob(null)}
                className="p-1 hover:bg-slate-800 rounded text-slate-400 hover:text-white"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            {/* Meta Attributes */}
            <div className="grid grid-cols-2 gap-4 text-xs">
              <div className="bg-slate-950 p-3 rounded-lg border border-slate-800">
                <span className="text-slate-500 block mb-1">Job Handler</span>
                <span className="font-semibold text-slate-200">{selectedJob.type}</span>
              </div>
              <div className="bg-slate-950 p-3 rounded-lg border border-slate-800">
                <span className="text-slate-500 block mb-1">Attempts Logged</span>
                <span className="font-mono text-slate-200">{selectedJob.attempts} / {selectedJob.maxRetries + 1}</span>
              </div>
            </div>

            {/* Error Message trace if failed */}
            {selectedJob.error && (
              <div className="bg-rose-950/30 border border-rose-500/30 rounded-lg p-3 text-xs text-rose-300 font-mono">
                <span className="font-bold block mb-1">Error Trace:</span>
                {selectedJob.error}
              </div>
            )}

            {/* Payload JSON view */}
            <div className="space-y-2">
              <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Payload Object</h4>
              <pre className="bg-slate-950 p-3 rounded-lg border border-slate-800 text-xs text-indigo-300 font-mono overflow-x-auto">
                {JSON.stringify(selectedJob.payload, null, 2)}
              </pre>
            </div>

            {/* Lifecycle Log History */}
            <div className="space-y-2">
              <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Execution Log Steps</h4>
              <div className="bg-slate-950 p-3 rounded-lg border border-slate-800 space-y-1 font-mono text-[11px] text-slate-300">
                {selectedJob.logs.map((log, idx) => (
                  <div key={idx} className="border-b border-slate-900/50 pb-1 last:border-none">
                    {log}
                  </div>
                ))}
              </div>
            </div>

            {/* Drawer Action Buttons */}
            <div className="pt-4 border-t border-slate-800 flex gap-3">
              <button
                onClick={() => {
                  queueEngine.retryJob(selectedJob.id);
                  setSelectedJob(null);
                }}
                className="flex-1 bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-semibold py-2 rounded-lg transition-all"
              >
                Re-Enqueue Task
              </button>
              <button
                onClick={() => {
                  queueEngine.deleteJob(selectedJob.id);
                  setSelectedJob(null);
                }}
                className="px-4 bg-slate-800 hover:bg-slate-700 text-rose-400 text-xs font-semibold py-2 rounded-lg transition-all"
              >
                Delete
              </button>
            </div>
          </div>
        </div>
      )}

    </div>
  );
}