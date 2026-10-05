const { Logger } = require('./logger');

class AgentOrchestrator {
  constructor(options = {}) {
    this.logger = options.logger || new Logger('AgentOrchestrator');
    this.defaultConcurrency = Math.max(
      1,
      Number(options.concurrency || process.env.AGENT_CONCURRENCY || 2)
    );
  }

  async runParallel(tasks, options = {}) {
    const entries = Object.entries(tasks || {});
    if (!entries.length) return {};

    const concurrency = Math.max(
      1,
      Math.min(Number(options.concurrency || this.defaultConcurrency), entries.length)
    );
    const failFast = options.failFast !== false;
    const results = {};
    const errors = {};
    let cursor = 0;
    let aborted = false;

    const worker = async () => {
      while (true) {
        if (aborted && failFast) return;
        const index = cursor++;
        if (index >= entries.length) return;

        const [name, task] = entries[index];
        const started = Date.now();
        try {
          results[name] = await task();
          this.logger.info(`Agent task completed: ${name} (${Date.now() - started}ms)`);
        } catch (error) {
          errors[name] = error;
          this.logger.warn(`Agent task failed: ${name}: ${error.message}`);
          if (failFast) aborted = true;
        }
      }
    };

    await Promise.all(Array.from({ length: concurrency }, () => worker()));

    if (Object.keys(errors).length) {
      const error = new Error(
        `Parallel agent stage failed: ${Object.keys(errors).join(', ')}`
      );
      error.code = 'AGENT_ORCHESTRATION_FAILED';
      error.causes = errors;
      throw error;
    }

    return results;
  }

  async runPool(items, worker, options = {}) {
    const values = Array.isArray(items) ? items : [];
    if (!values.length) return [];

    const concurrency = Math.max(
      1,
      Math.min(Number(options.concurrency || this.defaultConcurrency), values.length)
    );
    const results = new Array(values.length);
    let cursor = 0;

    const runWorker = async () => {
      while (true) {
        const index = cursor++;
        if (index >= values.length) return;
        results[index] = await worker(values[index], index);
      }
    };

    await Promise.all(Array.from({ length: concurrency }, () => runWorker()));
    return results;
  }
}

module.exports = { AgentOrchestrator };
