'use strict';
const mqtt = require('mqtt');
const Logger = require('@orchestra-platform/logger');

module.exports = class MqttManager {

    constructor(options = {}) {
        const { host, port = 1883, logLevel = Logger.LOG_LEVELS.WARN, username, password } = options;

        this.initialized = false;
        this._subscriptions = [];
        this._waitMessages = [];

        this._log = new Logger({
            logLevel: logLevel,
            name: 'MQTT Logger'
        });

        this._mqttClient = mqtt.connect(host, { port, username, password });
        this.initialized = new Promise((resolve, reject) => {
            this._mqttClient.on('connect', _ => {
                this._log.i(this.constructor.name, 'connection', 'connected', host);
                return resolve();
            });
        });
        this._mqttClient.on('reconnect', _ => {
            this._log.w(this.constructor.name, 'connection', 'reconnect', host);
        });
        this._mqttClient.on('close', _ => {
            this._log.e(this.constructor.name, 'connection', 'close', host);
        });
        this._mqttClient.on('error', error => {
            this._log.e(this.constructor.name, 'connection', 'error', [host, error]);
            // TODO: Send error notification
        });
        this._mqttClient.on('offline', _ => {
            this._log.w(this.constructor.name, 'connection', 'offline', host);
        });

        this._mqttClient.on('message', this._handleMessage.bind(this));
    }


    /**
     * Subscribe to a channel
     * @param {String} channel 
     * @param {Function} onMessageCallback 
     */
    subscribe(channel, onMessageCallback) {
        this._subscriptions.push({ channel, onMessageCallback });
        channel = channel + '/#';
        this._mqttClient.subscribe(channel);
    };


    /**
     * @param {String} topic 
     * @param {Object} msg 
     * @param {Object} options mqtt.publish options
     */
    publish(topic, msg, options = {}) {
        return new Promise((resolve, reject) => {
            const message = JSON.stringify(msg);
            this._mqttClient.publish(topic, message, options, err => {
                if (err)
                    return reject(err);
                resolve();
            });
        });
    }


    /**
     * @param {String} topic 
     */
    removeWaitMessage(topic) {
        const waitMessage = this._waitMessages.find(x => x.topic == topic);
        if (waitMessage) {
            clearTimeout(waitMessage.timeout);
            this._waitMessages = this._waitMessages.filter(x => x.topic !== topic);
        }
    }


    /**
     * @param {String} topic 
     * @param {Number} [timeoutMillis=500000] 
     */
    waitMessage(topic, timeoutMillis = 500000) {
        let resolveCallback, rejectCallback;
        const promise = new Promise((resolve, reject) => {
            resolveCallback = value => {
                this.removeWaitMessage(topic);
                resolve(value);
            };
            rejectCallback = err => {
                this.removeWaitMessage(topic);
                reject(err);
            };;
        });

        // Check if there is already a wait for this topic
        const waitMessage = this._waitMessages.find(wm => wm.topic == topic);
        if (waitMessage) {
            return Promise.reject('There is already a wait for this topic');
        }

        const timeout = setTimeout(_ => {
            this.removeWaitMessage(topic);
            rejectCallback('Timeout');
        }, timeoutMillis);

        this._waitMessages.push({
            topic: topic,
            timeout: timeout,
            resolve: resolveCallback,
            reject: rejectCallback
        });

        return promise;
    }


    /**
     * @param {String} topic 
     * @param {String} message 
     */
    _handleMessage(topic, message) {
        const subscriptions = this._subscriptions
            .filter(s => topic.startsWith(s.channel) || s.channel == "#");

        if (subscriptions.length === 0) {
            this._log.i(this.constructor.name, '_handleMessage', 'Ignored message in', topic);
            return;
        }

        const msgString = message.toString() || "{}";
        let msg;
        try {
            msg = JSON.parse(message.toString());
        } catch (e) {
            msg = message.toString();
            throw new Error('Invalid msg (not a json)');
        }

        // Check for waitMessages
        for (let i = 0; i < this._waitMessages.length; i++) {
            if (topic.startsWith(this._waitMessages[i].topic)) {
                this._waitMessages[i].resolve(msg);
            }
        }

        for (let i = 0; i < this._subscriptions.length; i++) {
            if (topic.startsWith(this._subscriptions[i].channel) || this._subscriptions[i].channel == "#") {
                this._subscriptions[i].onMessageCallback(topic, msg);
                return;
            }
        }
    }

};
