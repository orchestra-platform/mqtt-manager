'use strict';
const mqtt = require('mqtt');
const Logger = require('@orchestra-platform/logger');

module.exports = class MqttManager {

    constructor(options = {}) {
        const { host, port = 1883, logLevel = Logger.LOG_LEVELS.WARN } = options;

        this.initialized = false;
        this._subscriptions = [];
        this._waitMessages = [];

        this._log = new Logger({
            logLevel: logLevel,
            name: 'MQTT Logger'
        });

        this._mqttClient = mqtt.connect(host, { port });
        this.initialized = new Promise((resolve, reject) => {
            this._mqttClient.on('connect',  _ => {
                this._log.i(this.constructor.name, 'connection', 'connected', host);
                return resolve();
            });
        });
        this._mqttClient.on('reconnect', _ => {
            this._log.w(this.constructor.name, 'connection', 'reconnect', host);
            this._subscriptions.forEach(sub => this._mqttClient.subscribe(sub.channel));
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


    subscribe(channel, onMessageCallback) {
        if (false === channel.startsWith('/'))
            channel = '/' + channel;
        this._subscriptions.push({ channel, onMessageCallback });
        channel = channel + '/#';
        this._mqttClient.subscribe(channel);
    };


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

        for (let i = 0; i < this._subscriptions.length; i++) {
            if (topic.startsWith(this._subscriptions[i].channel) || this._subscriptions[i].channel == "#") {
                this._subscriptions[i].onMessageCallback(topic, msg);
                return;
            }
        }
    }

};
