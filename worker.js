/*
 * @Author: Mathias.Je 
 * @Date: 2019-10-10 10:42:31 
 * @Last Modified by: Mathias.Je
 * @Last Modified time: 2019-10-28 15:28:59
 */
import db from './modules/meta';
import ft from './modules/fileTransfer';
import container from './modules/logger';
import path from 'path';
import pFilter from 'p-filter';
import pMap from 'p-map';
import PQueue from 'p-queue';
import s3 from './modules/s3ListObjects';

const logger = container.get('migcliLogger');

const CONTAINERS = ["dash", "hls", "mp4", "etc"];
// const CONTAINERS = ["etc"];

const cfURLTag = (strs, ...vars) => {
    const url = strs.reduce((prev, curr, idx) => prev + strs[idx] + (vars[idx] ? vars[idx] : ''), '');
    return url;
}

const fileTransferMng = async (meta, _db) => {
    if (meta === undefined) {
        return true;
    }

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
            logger.debug(`Skip this meta and report to 'D'`);
            await _db.report(meta.j_id, "D");

            return true;
        }
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
    logger.debug(`[${meta.contentId}] report "Y"`);

    return true;
}

const fileTransferIntf = async (meta, container, uploader, queue, continuationToken = undefined) => {
    /**
     * AWS S3 sdk의 `listObjectsV2`를 통해 list를 조회한다
     * continuationToken: next list request token.
     */
    let lists = await new s3().getBucketList(meta, container, continuationToken);

    let filteredList = await pFilter(lists.contents, async content => {
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

        return true;
    });

    await pMap(filteredList, async content => {
        // list의 모든 key를 CF url로 재구성 및 병렬 처리
        queue.add(async () => {
            let key = content.Key; 
            let url = cfURLTag`https://vod-${meta.channelId.toLowerCase()}.cdn.wavve.com/${key}?Policy=eyJTdGF0ZW1lbnQiOlt7IlJlc291cmNlIjoiKiIsIkNvbmRpdGlvbiI6eyJEYXRlTGVzc1RoYW4iOnsiQVdTOkVwb2NoVGltZSI6MTg2ODAyNzQzNH19fV0sInRpZCI6IjEyNDI1OTY1ODM0IiwidmVyIjoiMyJ9&Signature=CtgOOwLsfz6nXSb1j~r8nMs-R2jeScoctwduf-peOdJr-LffFWzrFiMpHq9LxdvhzGogYhbzAfyFpZwGTjj1K5DL0g5eBu8QpUQbjyQlX~l9sYZ6emgbkzQLhaXqlrgKyN9fibnEIBO6WaC0GO2t9nhRXp8BqPWjIVT5He6vc8~0AGZSgfPOtne7ps43m2rry4xernLg8afy7mSPLsw3-Ae12NYo9~T4uwFcMMnUfRyLfzQ6IavicCjml7Tq26YZW5WQuBEwTf~yGbQZIiFw2Ft1mKWCfx0MwizNTwllMjXsNCtvVFuSA2F9woan-MZHPV2qlVDHPsBALzO9JkpDhw__&Key-Pair-Id=APKAJ6KCI2B6BKBQMD4A`;

            await uploader.upload(url, key); 
            // logger.debug(`remain queue size: ${queue.size} / ${queue.pending} - uploaded ${key}`);
        });
    });

    if (lists.nextContinuationToken) {
        // 조회 한 S3 list가 마지막 list가 아니라면 마지막 list가 조회 될때 까지 recursive call
        return await fileTransferIntf(meta, container, uploader, queue, lists.nextContinuationToken);
    } else {
        return true;
    }
}

const start = async () => {
    logger.debug(`Started worker ${process.pid}`);
    
    let _db = new db();
    const meta = await _db.getMeta(process.pid);
    if (meta.length === 0) {
        return true;
    }

    logger.debug(`[${process.pid}] Started migrating [${meta[0].contentId}]`);
    process.send({pid: process.pid, jobId: meta[0].j_id, contentId: meta[0].contentId, startAt: Math.floor(Date.now() / 1000)});
    
    logger.debug(`get meta: ${JSON.stringify(meta, null, 4)}`);
    try {
        await fileTransferMng(meta[0], _db);
        logger.debug(`[${process.pid}] Finished migrating [${meta[0].contentId}]`);
    } catch (error) {
        await _db.report(meta[0].j_id, "F");
        logger.error(`[${meta[0].contentId}] report "F" in worker`);
        throw error;
    }
}

(async () => {
    try {
        await start();
        process.send(`Done.`);

        process.exit(0); // successful exit
    } catch (err) {
        logger.error(`Error occurred worker: ${err.stack}`);

        setTimeout(() => {
            process.exit(1); // abnormal exit 
        }, 1000);
    }
})();