/*
 * @Author: Mathias.Je 
 * @Date: 2019-10-14 17:04:00 
 * @Last Modified by: Mathias.Je
 * @Last Modified time: 2019-10-31 13:19:47
 */
import mysql from 'mysql2/promise';
import container from './logger';

const logger = container.get('migcliLogger');

class Meta {
    constructor() {
        let dbCfg = {
            host: process.env.AZURE_DB_HOST,
            user: process.env.AZURE_DB_USER,
            password: process.env.AZURE_DB_PASS,
            database: process.env.AZURE_DB_DATABASE,
            port: process.env.AZURE_DB_PORT,
            connectionLimit: 20
        };

        this.dbConnPool = new mysql.createPool(dbCfg);
    }

    async getMeta() {
        try {
            const connection = await this.dbConnPool.getConnection();

            try {
                await connection.beginTransaction();

                const [rows] = await connection.query(`
                    SELECT 
                        channelId, 
                        contentId, 
                        mediaVersion, 
                        acquire, 
                        j_id 
                    FROM 
                        backoffice_dump 
                    WHERE 
                        isMig = 'N' 
                    ORDER BY 
                        j_UpdatedAt DESC 
                    LIMIT 1 
                    FOR UPDATE`);
                if (rows.length > 0) {
                    await connection.query(`
                        UPDATE 
                            backoffice_dump 
                        SET 
                            isMig = "P" 
                        WHERE 
                            j_id = ${rows[0].j_id}
                    `);
                }
                await connection.commit();
                connection.release();
                return rows;
            } catch (err) {
                await connection.rollback();
                connection.release();
                throw err;
            }
        } catch (err) {
            logger.error(`!!! Cannot connect to getMeta !!! Error: ${err.stack}`);
            throw err;
        }
    }

    /**
     * TODO report
     * ToBeDone:
     *      a. isMig column에 대한 "F" | "D" | "Y" 를 update
     */
    async report(jId, state) {
        try {
            const connection = await this.dbConnPool.getConnection();

            try {
                await connection.beginTransaction();
                await connection.query(`
                    UPDATE
                        backOffice_dump
                    SET
                        isMig = "${state}"
                    WHERE
                        j_id = ${jId}
                `);
                await connection.commit();
                connection.release();
                return true;
            } catch (err) {
                await connection.rollback();
                connection.release();
                throw err;
            }
        } catch (err) {
            logger.error(`!!! Cannot connect to report !!! Error: ${err.stack}`);
            throw err;
        }
    }
}

export default Meta;