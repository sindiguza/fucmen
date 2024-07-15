import * as dgram from 'dgram';
import * as crypto from 'crypto';
import * as os from 'os';
import { EventEmitter } from 'events';
import * as uuid from 'node-uuid';
import * as Pack from 'node-pack';
import * as _ from 'lodash';
const hostName = os.hostname();
export class Network extends EventEmitter {
    constructor(options) {
        super();
        this.socket = null;
        this.destinations = new Set();
        // tslint:disable-next-line:member-ordering
        this.msgWaitingAckBuffers = new Map();
        // tslint:disable-next-line:member-ordering
        this.msgBuffers = new Map();
        this.address = options.address;
        this.port = options.port;
        this.key = options.key;
        this.reuseAddr = options.reuseAddr;
        this.instanceUuid = options.instanceUuid;
        this.dictionary = _.uniq((options.dictionary || []).concat(['event', 'iid', 'hostName', 'data']));
    }
    start() {
        if (this.socket) {
            return Promise.resolve();
        }
        return new Promise((resolve, reject) => {
            this.socket = dgram.createSocket({ type: 'udp4', reuseAddr: this.reuseAddr }, async (data, rinfo) => {
                try {
                    const obj = await this.decode(data, rinfo);
                    if (!obj) {
                        return false;
                    }
                    else if (obj.iid === this.instanceUuid) {
                        return false;
                    }
                    else if (obj.event && obj.data) {
                        this.emit(obj.event, obj.data, obj, rinfo);
                    }
                    else {
                        this.emit('message', obj, rinfo);
                    }
                }
                catch (err) {
                    this.emit('error', err);
                }
            });
            this.socket.bind(this.port, this.address, () => {
                try {
                    this.bonded(this.address);
                    resolve();
                }
                catch (e) {
                    this.emit('error', e);
                    reject(e);
                }
            });
        });
    }
    stop() {
        this.socket && this.socket.close();
        this.socket = null;
    }
    async send(event, ...data) {
        if (this.socket) {
            const [contents] = await this.prepareMessage(event, 0, false, ...data);
            await Promise.all([...this.destinations.values()].map((destination) => this.sendToDest(destination, contents)));
        }
    }
    async sendToDest(destination, messages, port) {
        if (!this.socket) {
            return;
        }
        const socket = this.socket;
        const destPort = port || this.port;
        for (let contents of messages) {
            await new Promise((resolve, reject) => {
                socket.send(contents, 0, contents.length, destPort, destination, (err, bytes) => err ? reject(err) : resolve(bytes));
            });
        }
    }
    async prepareMessage(event, maxRetries, requireAck, ...data) {
        const obj = {
            event: event,
            iid: uuid.parse(this.instanceUuid),
            hostName: hostName,
            data: data
        };
        const msg = await this.encode(obj);
        if (msg.length > 1008 || requireAck) {
            const chunks = _.chunk(msg, 990).map(c => Buffer.from(c));
            const num = chunks.length;
            if (num > 254) {
                throw new Error('Message ' + event + ' too long');
            }
            const msgId = uuid.v4({}, Buffer.allocUnsafe(19), 3);
            msgId.writeUInt8(num, 0);
            msgId.writeUInt8(requireAck ? 1 : 0, 2);
            const msgs = chunks.map(c => Buffer.concat([msgId, c], 19 + c.length));
            msgs.forEach((m, idx) => m.writeUInt8(idx, 1));
            if (requireAck) {
                const ackBuffers = new AckBuffers(msgs, requireAck, () => this.msgWaitingAckBuffers.delete(msgUid), maxRetries);
                const msgUid = uuid.unparse(msgId, 3);
                this.msgWaitingAckBuffers.set(msgUid, ackBuffers);
                return [msgs, ackBuffers.promise];
            }
            else {
                return [msgs, Promise.resolve()];
            }
        }
        else {
            return [[Buffer.concat([Buffer.from([0]), msg], msg.length + 1)], Promise.resolve()];
        }
    }
    encode(data) {
        return new Promise((resolve, reject) => {
            try {
                const tmp = this.key ? encrypt(Pack.encode(data, this.dictionary), this.key) : Pack.encode(data, this.dictionary);
                resolve(tmp);
            }
            catch (e) {
                reject(e);
            }
        });
    }
    decode(data, rinfo) {
        const decodeBuffer = (buf) => {
            try {
                const tmp = Pack.decode(this.key ? decrypt(buf, this.key) : buf, this.dictionary);
                tmp.iid = uuid.unparse(tmp.iid);
                return tmp;
            }
            catch (e) {
                return undefined;
            }
        };
        return new Promise((resolve, reject) => {
            try {
                const numPackets = data.readUInt8(0);
                if (!numPackets) {
                    resolve(decodeBuffer(data.slice(1)));
                }
                else if (numPackets === 255) {
                    // ACK packet
                    const msgId = uuid.unparse(data, 1);
                    const ackBuffers = this.msgWaitingAckBuffers.get(msgId);
                    if (ackBuffers) {
                        if (ackBuffers.processAckPacket(data, 16 + 1)) {
                            this.msgWaitingAckBuffers.delete(msgId);
                        }
                    }
                }
                else {
                    const idx = data.readUInt8(1);
                    const requireAck = data.readUInt8(2) ? true : false;
                    const msgId = uuid.unparse(data, 3);
                    const buffer = this.msgBuffers.get(msgId) || new MsgBuffer(numPackets);
                    buffer.buffers.set(idx, data.slice(19));
                    if (this.msgBuffers.size > 10) {
                        const oldBuffers = [...this.msgBuffers.entries()].filter((e) => e[1].isOld);
                        oldBuffers.forEach((e) => {
                            // console.log('Removing buffer => ' + e[1].buffers.size + '/' + e[1].numBuffers)
                            this.msgBuffers.delete(e[0]);
                        });
                    }
                    this.msgBuffers.set(msgId, buffer);
                    if (requireAck && this.socket) {
                        const ackBuf = Buffer.allocUnsafe(1 + 16 + Math.floor(256 / 8));
                        ackBuf.writeUInt8(255, 0);
                        uuid.parse(msgId, ackBuf, 1);
                        ackBuf.fill(0, 17);
                        const okPackets = new Set([...buffer.buffers.keys()]);
                        for (let i = 0; i < numPackets; ++i) {
                            if (okPackets.has(i)) {
                                const oldVal = ackBuf.readUInt8(1 + 16 + Math.floor(i / 8));
                                ackBuf.writeUInt8(oldVal | (1 << (i % 8)), 1 + 16 + Math.floor(i / 8));
                            }
                        }
                        this.socket.send(ackBuf, 0, ackBuf.length, rinfo.port, rinfo.address);
                    }
                    if (buffer.buffers.size === numPackets) {
                        this.msgBuffers.delete(msgId);
                        const fullMsg = Buffer.concat(_.sortBy([...buffer.buffers.entries()], (e) => e[0]).map((e) => e[1]));
                        resolve(decodeBuffer(fullMsg));
                    }
                    resolve(undefined);
                }
            }
            catch (e) {
                reject(e);
            }
        });
    }
}
class AckBuffers {
    constructor(buffers, cbk, cbkReject, maxRetries) {
        this.buffers = new Map();
        this.retries = 0;
        this.maxRetries = maxRetries;
        buffers.forEach((buf, idx) => this.buffers.set(idx, buf));
        this.promise = new Promise((resolve, reject) => {
            this.resolve = resolve;
            this.reject = reject;
        });
        this.startTimer(cbk, cbkReject);
    }
    processAckPacket(data, offset) {
        [...this.buffers.keys()].forEach((k) => {
            const ackVal = data.readUInt8(offset + Math.floor(k / 8));
            if (ackVal & (1 << (k % 8))) {
                this.buffers.delete(k);
            }
        });
        const completed = (this.buffers.size === 0);
        if (completed) {
            clearTimeout(this.timer);
            this.resolve();
        }
        return completed;
    }
    startTimer(cbk, cbkReject) {
        this.timer = setTimeout(() => {
            if (++this.retries >= this.maxRetries) {
                this.buffers.clear();
                cbkReject();
                this.reject(new Error(`Too many retries: ${this.maxRetries}`));
            }
            else {
                cbk([...this.buffers.values()]);
                this.startTimer(cbk, cbkReject);
            }
        }, 800);
    }
}
class MsgBuffer {
    constructor(numBuffers) {
        this.buffers = new Map();
        this.arrivedAt = process.hrtime();
        this.numBuffers = numBuffers;
    }
    compare(other) {
        if (this.arrivedAt[0] === other.arrivedAt[0]) {
            return this.arrivedAt[1] - other.arrivedAt[1];
        }
        else {
            return this.arrivedAt[0] - other.arrivedAt[0];
        }
    }
    get isOld() {
        const age = process.hrtime(this.arrivedAt);
        // If older than 10 second
        return age[0] >= 10;
    }
}
export class MulticastNetwork extends EventEmitter {
    constructor(multicastAddress = '224.0.2.1', ttl = 1, options) {
        super();
        this.multicastAddress = multicastAddress;
        this.ttl = ttl;
        this.networks = [];
        this.options = Object.assign({}, options);
    }
    async start() {
        if (this.networks.length) {
            return Promise.resolve();
        }
        if (this.options.address.length === 0 || this.options.address === '0.0.0.0') {
            const ifaces = _.flatten(_.values(os.networkInterfaces())).filter(x => x.family === 'IPv4' && !x.internal).map(x => x.address);
            this.networks = ifaces.map((ifaddress) => new MulticastNetworkInternal(this.multicastAddress, this.ttl, Object.assign(Object.assign({}, this.options), { address: ifaddress })));
        }
        else {
            this.networks.push(new MulticastNetworkInternal(this.multicastAddress, this.ttl, this.options));
        }
        await Promise.all(this.networks.map((n) => n.start()));
    }
    stop() {
        this.networks.forEach((n) => n.stop());
        this.networks = [];
    }
    async send(event, ...data) {
        try {
            await Promise.all(this.networks.map((n) => n.send(event, ...data)));
        }
        catch (e) {
            console.error(e);
            throw e;
        }
    }
}
class MulticastNetworkInternal extends Network {
    constructor(multicastAddress = '224.0.2.1', ttl = 1, options) {
        super(options);
        this.multicastTTL = 1;
        this.multicast = multicastAddress;
        this.multicastTTL = ttl;
    }
    bonded(address) {
        // addMembership can throw if there are no interfaces available
        this.socket && this.socket.addMembership(this.multicast, address);
        this.socket && this.socket.setMulticastInterface(address);
        this.socket && this.socket.setMulticastTTL(this.multicastTTL);
        this.destinations.add(this.multicast);
    }
}
export class BroadcastNetwork extends Network {
    constructor(broadcastAddress = '255.255.255.255', options) {
        super(options);
        this.broadcast = broadcastAddress;
    }
    bonded(address) {
        this.socket && this.socket.setBroadcast(true);
        this.destinations.add(this.broadcast);
    }
}
export class DynamicUnicastNetwork extends Network {
    constructor(options, unicastAddresses) {
        super(options);
        if (typeof unicastAddresses === 'string') {
            if (~unicastAddresses.indexOf(',')) {
                this.unicast = unicastAddresses.split(',');
            }
            else {
                this.unicast = [unicastAddresses];
            }
        }
        else {
            this.unicast = unicastAddresses || [];
        }
    }
    add(address) {
        this.destinations.add(address);
    }
    remove(address) {
        this.destinations.delete(address);
    }
    async sendTo(destination, port, event, maxRetries, ...data) {
        if (this.socket) {
            const [contents, completed] = await this.prepareMessage(event, maxRetries, maxRetries ? async (buffers) => {
                await this.sendToDest(destination, buffers, port);
            } : false, ...data);
            await this.sendToDest(destination, contents, port);
            return completed;
        }
    }
    bonded(address) {
        this.destinations = new Set(this.unicast);
    }
}
function encrypt(data, key) {
    const cipher = crypto.createCipher('aes256', key);
    const buf = [];
    buf.push(cipher.update(data));
    buf.push(cipher.final());
    return Buffer.concat(buf);
}
function decrypt(data, key) {
    const decipher = crypto.createDecipher('aes256', key);
    const buf = [];
    buf.push(decipher.update(data));
    buf.push(decipher.final());
    return Buffer.concat(buf);
}
//# sourceMappingURL=network.js.map