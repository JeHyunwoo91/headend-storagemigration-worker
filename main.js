/*
 * @Author: Mathias.Je 
 * @Date: 2019-10-10 10:41:03 
 * @Last Modified by: Mathias.Je
 * @Last Modified time: 2019-11-06 11:19:55
 */
const cp = require('child_process');
const container = require('./modules/logger');
const dotenv = require('dotenv');
const iDB = require('./modules/meta');
dotenv.config();

const logger = container.get('migcliLogger');

let workingJob = new Array();

const sleep = (sec) => {
    return new Promise((resolve, reject) => {
        setTimeout(resolve, parseInt(sec) * 1000);
    });
}

const jobHandler = async (pid, code, signal) => {
    const db = new iDB();
    let idx = workingJob.findIndex(job => job.pid === pid);
    if (idx > -1) {
        let { jobId, contentId } = workingJob[idx];

        workingJob.splice(idx, 1);
        
        if (code !== 0 || signal !== null) {
            logger.error(`${pid} return code ${code} and signal ${signal}`);
            await db.report(jobId, "F");
            logger.error(`[${pid} - ${contentId}] report "F" from main`);
        }
    }
}

const createWorker = async () => {
    let worker = cp.fork('worker.js');
    let SLEEP_INTERVAL = 1;
    logger.debug(`Created worker ${worker.pid}`);

    worker.on('message', async (msg) => {
        if (msg instanceof Object) { // worker getting new job
            logger.debug(`Message from ${worker.pid} : ${JSON.stringify(msg)}`);
            workingJob.push(msg);
            logger.debug(`${worker.pid} push to workingJob`);
        } else if (msg === "HB") { // checking worker HeartBeat every seconds
            let idx = workingJob.findIndex(job => job.pid == worker.pid);
            if (idx > -1) { // exist worker
                let currTime = Math.floor(Date.now() / 1000);
                workingJob[idx].hb = currTime;
            } else { // non-exist worker
                logger.error(`Invalid worker ${worker.pid}`);

                await jobHandler(worker.pid, 1, null);

                await sleep(SLEEP_INTERVAL);

                createWorker();
            }
        } else if (msg === "UO") { // Upload Overload
            worker.disconnect();
            logger.error(`[${worker.pid}] Immediately disconnect with worker...`);
            worker.kill(9);
        }
    });

    worker.on('exit', async (code, signal) => {
        logger.debug(`worker ${worker.pid} exit code(${code}) / signal(${signal})`);
        
        await jobHandler(worker.pid, code, signal);

        if (signal === "SIGKILL" || signal === "SIGABRT") {
            logger.error(`Waiting for create new worker until Network stabilize`);
            SLEEP_INTERVAL = process.env.SLEEP_INTERVAL;
        }
        
        await sleep(SLEEP_INTERVAL);

        createWorker();
    });

    worker.on('error', async (err) => {
        logger.error(`Event Emitted error: ${err.stack}`);

        await jobHandler(worker.pid, 1, null);

        SLEEP_INTERVAL = process.env.SLEEP_INTERVAL;
        await sleep(SLEEP_INTERVAL);

        createWorker();
    });
}

const checkWorkingJob = () => {
    setInterval(async () => {
        logger.debug(`check workingJob(${workingJob.length}): ${JSON.stringify(workingJob)}`);

        // Jobs that have been delayed for more than 2 hours(7200s) are treated as delayedJob.
        let currTime = Math.floor(Date.now() / 1000);
        let abnormalJobs = workingJob.filter(job => ((parseInt(job.startAt) + parseInt(process.env.WORKER_MAXIMUM_ALIVE_DURATION)) < currTime || (job.hb + 60) < currTime));
        if (abnormalJobs.length > 0) {
            logger.debug(`over 4hour delayed job(${abnormalJobs.length}): ${JSON.stringify(abnormalJobs)}`);

            abnormalJobs.map(async (job) => {
                await jobHandler(job.pid, 1, null);
            });
        }
    }, 60000);
}

function run() {
    logger.debug("start migcli");
    logger.debug(`running worker count: ${process.env.NUMBER_OF_WORKER}`);
    let workerSet = new Array(parseInt(process.env.NUMBER_OF_WORKER)).fill(0);
    checkWorkingJob();
    
    workerSet.forEach(() => {
        setTimeout(() => {
            createWorker();
        }, (Math.floor(Math.random() * 5) + 1) * 1000);
    });
}

run();