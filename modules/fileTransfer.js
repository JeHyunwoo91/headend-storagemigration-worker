/*
 * @Author: Mathias.Je 
 * @Date: 2019-10-17 10:18:58 
 * @Last Modified by: Mathias.Je
 * @Last Modified time: 2019-11-06 12:40:21
 */
const container = require('./logger');
const http = require('http');
const https = require('https');
const mime = require('mime');
const path = require('path');

const request = require('axios');
const retry = require('promise-retry');
const {
    Aborter,
    BlockBlobURL,
    ContainerURL,
    ServiceURL,
    SharedKeyCredential,
    StorageURL,
    uploadStreamToBlockBlob
} = require('@azure/storage-blob');
// const az = require('@azure/storage-blob');

const logger = container.get('migcliLogger');

const CONTAINER_NAME = process.env.AZURE_STORAGE_CONTAINER_NAME;

const ONE_MEGABYTE = 1024 * 1024;
const FOUR_MEGABYTES = 4 * ONE_MEGABYTE;
const TEN_MEGABYTES = 10 * ONE_MEGABYTE;

/**
 * TODO FileTransfer
 * ToBeDone:
 *      a. 원본 파일 사이즈, upload 된 파일 사이즈 비교를 위한 queue push
 */
class FileTransfer {
    constructor() {
        let STORAGE_ACCOUNT_NAME = process.env.AZURE_STORAGE_ACCOUNT_NAME;
        let STORAGE_ACCOUNT_ACCESS_KEY = process.env.AZURE_STORAGE_ACCOUNT_ACCESS_KEY;
        // Shared Key Authorization based on account name and account key
        let credential = new SharedKeyCredential(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_ACCESS_KEY);
        let pipeline = StorageURL.newPipeline(credential);

        this.serviceURL = new ServiceURL(`https://${STORAGE_ACCOUNT_NAME}.blob.core.windows.net`, pipeline);
        this.containerURL = ContainerURL.fromServiceURL(this.serviceURL, CONTAINER_NAME);
        this.aborter = Aborter.none;

        this.options = {
            retries: 5,
            factor: 2,
            minTimeout: 1000,
            maxTimeout: 30000,
            timeout: 2000,
            localTest: null
        };
    }

    /**
     * TODO uploadStream
     * @description fileTransferIntf로 부터 전달 된 Signed AWS CloudFront URL을 지정 된 key 값으로 Azure Storage로 upload
     * ToBeDone:
     *      a. url을 read stream으로 pipe하여 uploadStreamToBlockBlob()의 parameter로 전달
     */
    async _upload(aborter, containerURL, url, key) {
        // console.log(`start upload ${key}`);
        const blockBlobURL = BlockBlobURL.fromContainerURL(containerURL, key);
        let stream = await request({
            method: 'get',
            url: url,
            responseType: 'stream',
        });
        
        const uploadOptions = {
            bufferSize: parseInt(process.env.UPLOAD_BUFFER_SIZE),
            maxBuffers: parseInt(process.env.UPLOAD_MAX_BUFFERS),
        };
        
        await uploadStreamToBlockBlob(
            aborter,
            stream.data,
            blockBlobURL,
            uploadOptions.bufferSize,
            uploadOptions.maxBuffers,
            {
                // progress: ev => logger.debug(`uploadStream ev: ${JSON.stringify(ev)}`),
                blobHTTPHeaders: { blobContentType: mime.getType(path.extname(key)) }
            }
        );
    }
    
    async upload(url,key){
        return await this._retry(this._upload,  this.aborter, this.containerURL, url, key);
    }
    
    async _retry(fn, ...args) {
        const options = {
            retries: this.options.retries,
            factor: this.options.factor,
            minTimeout: this.options.minTimeout,
            maxTimeout: this.options.maxTimeout
        };

        if (this.options.retries === 0) {
            return await fn.apply(null, args);
        }
        
        return await retry(options, async (retry, number) => {
            try {
                return await fn.apply(null, args);
            } catch (err) {
                // logger.error(`[${err.config.url.split('?')[0]}] Attempt ${number} failed. There are ${options.retries - (number - 1)} retries left.`);
                throw retry(err);
            }
        });
    }
}

module.exports = FileTransfer;