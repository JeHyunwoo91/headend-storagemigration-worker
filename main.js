/*
 * @Author: Mathias.Je 
 * @Date: 2019-10-10 10:41:03 
 * @Last Modified by: Mathias.Je
 * @Last Modified time: 2019-10-30 18:33:58
 */
import { fork } from 'child_process';
import container from './modules/logger';
import dotenv from 'dotenv';
import iDB from './modules/meta';
import os from 'os';
dotenv.config();

const logger = container.get('migcliLogger');

let workingJob = new Array();

const jobHandler = async (pid, status) => {
    const db = new iDB();
    let idx = workingJob.findIndex(job => job.pid === pid);
    if (idx > -1) {
        if (status !== 0) {
            logger.error(`${pid} return code ${status instanceof Object ? JSON.stringify(status, null, 4) : status}`);
            await db.report(workingJob[idx].jobId, "F");
            logger.error(`[${workingJob[idx].contentId}] report "F" from main`);
        }
        
        workingJob.splice(idx, 1);
    }

    createWorker();
}

const createWorker = async () => {
    let worker = fork('worker.js');
    logger.debug(`Created worker ${worker.pid}`);
    

    worker.on('message', (msg) => {
        if (msg instanceof Object) {
            logger.debug(`Message from ${worker.pid} : ${JSON.stringify(msg)}`);
            workingJob.push(msg);
            logger.debug(`${worker.pid} push to workingJob`);
        } else if (msg === "hb") {
            let idx = workingJob.findIndex(job => job.pid == worker.pid);
            if (idx > -1) {
                let currTime = Math.floor(Date.now() / 1000);
                workingJob[idx].hb = currTime;
            } else {
                logger.error(`Invalid worker ${worker.pid}`);

                (async () => await jobHandler(worker.pid, 1))();
            }
        }
    });

    worker.on('exit', async (code, signal) => {
        logger.debug(`worker ${worker.pid} exit code(${code}) / signal(${signal})`);
        
        await jobHandler(worker.pid, code);
    });

    worker.on('error', async (err) => {
        logger.error(`Event Emitted error: ${err.stack}`);

        await jobHandler(worker.pid, 1);
    });
}

const checkWorkingJob = () => {
    setInterval(async () => {
        logger.debug(`check workingJob(${workingJob.length}): ${JSON.stringify(workingJob)}`);

        // Jobs that have been delayed for more than 4 hours(14400s) are treated as delayedJob.
        let currTime = Math.floor(Date.now() / 1000);
        let abnormalJobs = workingJob.filter(job => ((job.startAt + 14400) < currTime || (job.hb + 60) < currTime));
        if (abnormalJobs.length > 0) {
            logger.debug(`over 4hour delayed job(${abnormalJobs.length}): ${JSON.stringify(abnormalJobs)}`);

            abnormalJobs.map(async (job) => {
                await jobHandler(job.pid, 1);
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