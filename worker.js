/*
 * @Author: Mathias.Je 
 * @Date: 2019-10-10 10:42:31 
 * @Last Modified by: Mathias.Je
 * @Last Modified time: 2019-11-06 09:50:48
 */
import db from './modules/meta';
import EventEmitter from 'eventemitter3';
import ft from './modules/fileTransfer';
import container from './modules/logger';
import path from 'path';
import pMap from 'p-map';
import PQueue from 'p-queue';
import s3 from './modules/s3ListObjects';

const logger = container.get('migcliLogger');
const queueEventEmitter = new EventEmitter();

const fileTransferMng = async (meta, _db) => {
    /**
     * 미디어버젼이 1이상이 되지만 최근 미디어버젼 입수 시 실패(acquire = F)한 case에 대해서는
     * 이전(mediaVersion--) 미디어버젼에 해당하는 파일을 이전한다.
     * 단, 미디어버젼이 2 이상인 meta에 한하며, 미디어버젼 1일 때 입수 실패가 
     * 발생한 경우에 대해 skip 한다.
     */
    if (meta.acquire === "F") {
        let mv = parseInt(meta.mediaVersion);
        if (mv >= 2) {
            meta.mediaVersion = mv - 1;
        } else {
            // report "D" and exit
            logger.debug(`[${process.pid} - ${meta.contentId}]Skip this meta and report to 'D'`);
            await _db.report(meta.j_id, "D");

            return true;
        }
    }
    let CONTAINERS = ["dash", "hls", "mp4", "etc"];
    if (process.env.ISDRM == "N") {
        CONTAINERS = ["mp4", "etc"];
    }

    await pMap(CONTAINERS, async container => {
        let concurrency = container === "mp4" ? 
            parseInt(process.env.CONCURRENCY_LEVEL_OF_MP4) : 
            parseInt(process.env.CONCURRENCY_LEVEL_OF_OTHERS);
        
        const queue = new PQueue({ concurrency: concurrency });
        const uploader = new ft();

        logger.debug(`started move [${container}/${meta.contentId}]`);


        await fileTransferIntf(meta, container, uploader, queue);
        await queue.onIdle();
        logger.debug(`[${container}/${meta.contentId}] All files moved`);
    });
    
    await _db.report(meta.j_id, "Y");
    logger.debug(`[${process.pid} - ${meta.contentId}] report "Y"`);

    return true;
}

const fileTransferIntf = async (meta, container, uploader, queue, continuationToken = undefined) => {
    /**
     * AWS S3 sdk의 `listObjectsV2`를 통해 list를 조회한다
     * continuationToken: next list request token.
     */
    let lists = await new s3().getBucketList(meta, container, continuationToken);

    await pMap(lists.contents, async content => {
        let key = content.Key;
        if (path.extname(key).length <= 2) {
            return false;
        }

        let keys = key.split('/');

        // migration skipped condition
        // dash / hls container의 watermark index가 `1`인 sub-directory만 이전
        if (container === 'dash' && keys[5] === 'video') {
            if (keys[6] !== '1') {
                // logger.debug(`skip excluded index of watermark sub-directory`);
                return false;
            }
        } else if (container === 'hls') {
            if (keys[5] === '2' || keys[5] === '3') {
                // logger.debug(`skip excluded index of watermark sub-directory`);
                return false;
            }
        } else if (container === 'etc' && keys[3] !== "contentinfo.json") { // 현재 미디어버젼의 이전 sub-directory는 skip. contentInfo.json은 반드시 이전.
            if (keys[3] < meta.mediaVersion) {
                return false;
            }
        }

        queue.add(async () => {
            // let key = content.Key; 
            let channelId = meta.channelId;
            let url = `https://vod-${channelId}.cdn.wavve.com/${key}?Policy=${process.env.POLICY}`;
            
            await uploader.upload(url, key);
            // console.log(`remain queue size: ${queue.size} / ${queue.pending} - uploaded ${key}`);
        }).catch(error => queueEventEmitter.emit('error', key, error));
    });

    if (lists.nextContinuationToken) {
        // 조회 한 S3 list가 마지막 list가 아니라면 마지막 list가 조회 될때 까지 recursive call
        // return await fileTransferIntf(meta, container, uploader, queue, lists.nextContinuationToken);
        return await fileTransferIntf(meta, container, uploader, queue, lists.nextContinuationToken);
    } else {
        return true;
    }
}

const start = async () => {
    logger.debug(`[${process.pid}]started worker`);
    
    let _db = new db();
    const meta = await _db.getMeta(process.pid);
    if (meta.length === 0) {
        return true;
    }

    logger.debug(`[${process.pid}] Get meta: ${JSON.stringify(meta, null, 4)}`);

    const contentId = meta[0].contentId;

    logger.debug(`[${process.pid}] Started migrating [${contentId}]`);
    let currTime = Math.floor(Date.now() / 1000);
    process.send({
        pid: process.pid, 
        jobId: meta[0].j_id, 
        contentId: contentId, 
        startAt: currTime,
        hb: currTime
    });

    hb();
    try {
        await fileTransferMng(meta[0], _db);
        queueEventEmitter.emit('end', contentId);
    } catch (error) {
        await _db.report(meta[0].j_id, "F");
        logger.error(`[${process.pid} - ${contentId}] report "F" in worker`);
        throw error;
    }
}

const hb = () => {
    setInterval(() => {
        process.send('HB');
    }, 1000);
}

const sleep = (sec) => {
    return new Promise((resolve, reject) => {
        setTimeout(resolve, parseInt(sec) * 1000);
    });
}

(async () => {
    try {
        queueEventEmitter.on('error', async (key, error) => {
            logger.error(`[${process.pid} - ${key}] enqueued upload Job Error: ${error.message}`);
            process.send("UO");
        });

        queueEventEmitter.on('end', (contentId) => {
            logger.debug(`[${process.pid}] Finished migrating [${contentId}]`);

            process.exit(0); // abnormal exit 
        });
        
        await start();
        process.send(`Done.`);

        process.exit(0); // successful exit
    } catch (err) {
        logger.error(`Error occurred worker: ${err.message}`);
        
        process.exit(1); // abnormal exit 
    }
})();