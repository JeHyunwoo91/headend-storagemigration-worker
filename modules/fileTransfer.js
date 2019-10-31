/*
 * @Author: Mathias.Je 
 * @Date: 2019-10-17 10:18:58 
 * @Last Modified by: Mathias.Je
 * @Last Modified time: 2019-10-31 13:50:32
 */
import container from './logger';
import http from 'http';
import https from 'https';
import mime from 'mime';
import path from 'path';
import request from 'axios';
import {
    Aborter,
    BlockBlobURL,
    ContainerURL,
    ServiceURL,
    SharedKeyCredential,
    StorageURL,
    uploadStreamToBlockBlob
} from '@azure/storage-blob';

const logger = container.get('migcliLogger');

const CONTAINER_NAME = process.env.AZURE_STORAGE_CONTAINER_NAME;

const ONE_MEGABYTE = 1024 * 1024;
const FOUR_MEGABYTES = 4 * ONE_MEGABYTE;
const TEN_MEGABYTES = 10 * ONE_MEGABYTE;
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 100 });

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
    }

    /**
     * TODO uploadStream
     * @description fileTransferIntf로 부터 전달 된 Signed AWS CloudFront URL을 지정 된 key 값으로 Azure Storage로 upload
     * ToBeDone:
     *      a. url을 read stream으로 pipe하여 uploadStreamToBlockBlob()의 parameter로 전달
     */
    async upload(url, key) {
        // logger.debug(`start upload ${key}`);
        const blockBlobURL = BlockBlobURL.fromContainerURL(this.containerURL, key);
        let stream = await request({
            method: 'get',
            url: url,
            responseType: 'stream',
            httpsAgent: httpsAgent
        });
        
        const uploadOptions = {
            bufferSize: parseInt(process.env.UPLOAD_BUFFER_SIZE),
            maxBuffers: 5,
        };

        await uploadStreamToBlockBlob(
            this.aborter,
            stream.data,
            blockBlobURL,
            uploadOptions.bufferSize,
            uploadOptions.maxBuffers,
            {
                // progress: ev => logger.debug(`uploadStream ev: ${JSON.stringify(ev)}`),
                blobHTTPHeaders: { blobContentType: mime.getType(path.extname(key)) }
            }
        );
        
        // logger.debug(`uploaded ${key}`);
    }
}

export default FileTransfer;