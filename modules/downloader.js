/*
 * @Author: Mathias.Je 
 * @Date: 2019-11-02 16:33:31 
 * @Last Modified by: Mathias.Je
 * @Last Modified time: 2019-11-04 09:25:10
 */
import axios from 'axios';
import https from 'https';

const httpsAgent = new https.Agent({
    keepAlive: true,
    maxSockets: 100,
});

const MAX_REQUESTS_COUNT = process.env.MAX_REQUESTS_COUNT;
const REQUEST_INTERVAL_MS = process.env.REQUEST_INTERVAL_MS;
let PENDING_REQUESTS = 0;

const downloader = axios.create({httpsAgent: httpsAgent});

downloader.interceptors.request.use(config => {
    return new Promise((resolve, reject) => {
        let interval = setInterval(() => {
            if (PENDING_REQUESTS < MAX_REQUESTS_COUNT) {
                PENDING_REQUESTS++;
                clearInterval(interval);
                resolve(config);
            }
        }, REQUEST_INTERVAL_MS);
    });
});

downloader.interceptors.response.use(response => {
    PENDING_REQUESTS = Math.max(0, PENDING_REQUESTS - 1);
    return Promise.resolve(response);
}, error => {
    PENDING_REQUESTS = Math.max(0, PENDING_REQUESTS - 1);
    return Promise.reject(error);
});

export default downloader;