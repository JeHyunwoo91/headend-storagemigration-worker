/*
 * @Author: Mathias.Je 
 * @Date: 2019-10-19 15:50:14 
 * @Last Modified by: Mathias.Je
 * @Last Modified time: 2019-10-25 11:26:55
 */
import AWS from 'aws-sdk';

class S3ListObjects {
    constructor() {
        let options = {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID,
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
            region: process.env.AWS_REGION
        };
        this.s3 = new AWS.S3(options);
    }

    /**
     * TODO getBucketList
     * @param meta: 버킷 및 prefix 정의를 위한 meta 정보
     * @param container prefix key 문자열에 tag 될 container 정보(dash | hls | mp4 | etc)
     * @param continuationToken 버킷 및 prefix에 해당하는 list 중, maxKeys limit에 의해 다음 list를 이어서 조회하기 위한 token
     * 
     * ToBeDone:
     */
    async getBucketList(meta, container, continuationToken) {
        let params = {
            Bucket: `pooqcdn-vod-${meta.channelId.toLowerCase()}`,
            Prefix: `${container}/${meta.channelId.toUpperCase()}/${meta.contentId}/${container === "etc" ? "" : meta.mediaVersion + "/"}`,
            MaxKeys: 200,
            ContinuationToken: continuationToken
        };

        let list = await this.s3.listObjectsV2(params).promise();

        /**
         * Contents: metadata about each object returned.
         * NextContinuationToken: next list request token.
         */
        return { contents: list.Contents, nextContinuationToken: list.NextContinuationToken };
    }
}

export default S3ListObjects;