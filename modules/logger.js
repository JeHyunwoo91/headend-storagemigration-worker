/*
 * @Author: Mathias.Je 
 * @Date: 2019-10-19 14:21:43 
 * @Last Modified by: Mathias.Je
 * @Last Modified time: 2019-11-06 11:13:34
 */
const { createLogger, format, transports, loggers, Container } = require('winston');
require('winston-daily-rotate-file');
const fs = require('fs');
const moment = require('moment-timezone');

const { combine, printf } = format;
const BASE_LOG_PATH = "/data/log/migcli/";

if (!fs.existsSync(BASE_LOG_PATH)) {
    fs.mkdirSync(BASE_LOG_PATH);
}

const defaultOptions = {
    level: 'debug',
    dirname: BASE_LOG_PATH,
    filename: '',
    datePattern: "YYYY-MM-DD",
    handleExceptions: true,
    json: false,
    maxSize: 20 * 1024 * 1024, // 20M
    maxFiles: "7d",
    auditFile: `/dev/null`,
    createSymlink: true,
    symlinkName: ''
};

const dailyRotateDbgFileTransport = new (transports.DailyRotateFile)({ ...defaultOptions, level: 'debug', filename: `debug-%DATE%.log`, symlinkName: 'debug.log' });
const dailyRotateErrFileTransport = new (transports.DailyRotateFile)({ ...defaultOptions, level: 'error', filename: `error-%DATE%.log`, symlinkName: 'error.log' });
const dailyRotateExcFileTransport = new (transports.DailyRotateFile)({ ...defaultOptions, level: 'error', filename: `exception-%DATE%.log`, symlinkName: 'exception.log' });

const customFormat = printf(({ level, message }) => 
    `${timezoned()} ${level} ▶ ${message}`
);

const container = new Container();

container.add('migcliLogger', {
    format: combine(
        customFormat
    ),
    transports: [
        dailyRotateDbgFileTransport,
        dailyRotateErrFileTransport,
    ],
    exceptionHandlers: [
        dailyRotateExcFileTransport,
    ]
});

// Azure VM은 KST 설정이 불가하여 custom timestamp format을 사용한다.
const timezoned = () => moment().tz('Asia/Seoul').format("YYYY-MM-DD HH:mm:ss.SS");

module.exports = container;