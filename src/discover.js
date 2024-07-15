import { BroadcastNetwork, MulticastNetwork, DynamicUnicastNetwork } from './network';
import { EventEmitter } from 'events';
import * as os from 'os';
import * as uuid from 'node-uuid';
import * as _ from 'lodash';
const reservedEvents = ['promotion', 'demotion', 'added', 'removed', 'master', 'hello', 'direct'];
var MulticommMode;
(function (MulticommMode) {
    MulticommMode[MulticommMode["Broadcast"] = 0] = "Broadcast";
    MulticommMode[MulticommMode["Multicast"] = 1] = "Multicast";
    MulticommMode[MulticommMode["Unicast"] = 2] = "Unicast";
})(MulticommMode || (MulticommMode = {}));
class Node {
    constructor(hijackLocal) {
        this.hijackLocal = hijackLocal;
        this.isMaster = false;
        this.weight = -Infinity;
        this.prefMode = null;
        this.advertisement = undefined;
        this._lastSeenBroadcast = 0;
        this._lastSeenMulticast = 0;
    }
    get address() {
        return this._address;
    }
    set address(address) {
        this._address = this.hijackLocal && isLocalIP(address) ? '127.0.0.1' : address;
    }
    get lastSeen() {
        return Math.max(this._lastSeenBroadcast, this._lastSeenMulticast);
    }
    set lastSeenBroadcast(value) {
        this._lastSeenBroadcast = value;
    }
    set lastSeenMulticast(value) {
        this._lastSeenMulticast = value;
    }
    preferredMode(timeout) {
        if (this._address === '127.0.0.1') {
            return MulticommMode.Unicast;
        }
        else if (this.prefMode !== null) {
            return this.prefMode;
        }
        else if (+new Date() - this._lastSeenBroadcast <= timeout) {
            return MulticommMode.Broadcast;
        }
        else {
            return MulticommMode.Multicast;
        }
    }
}
class HelloData {
    constructor() {
        this.isMaster = false;
    }
}
export class DiscoverOptions {
}
export class Discover extends EventEmitter {
    constructor(options, advertisement) {
        super();
        this.helloInterval = 1000;
        this.checkInterval = 2000;
        this.resetInterval = 60000;
        this.nodeTimeout = 2000;
        this.masterTimeout = 2000;
        this.mastersRequired = 1;
        this.me = new HelloData();
        this.running = false;
        this.nodes = new Map();
        this.channels = new Set();
        this.instanceUuid = uuid.v4();
        this.broadcastHelloCounter = 0;
        this.multicastHelloCounter = 0;
        this.helloInterval = options.helloInterval || 1000;
        this.checkInterval = options.checkInterval || 2000;
        this.nodeTimeout = options.nodeTimeout || 2000;
        this.masterTimeout = options.masterTimeout || 2000;
        this.mastersRequired = options.mastersRequired || 1;
        if (this.nodeTimeout < this.checkInterval) {
            throw new Error('nodeTimeout must be greater than or equal to checkInterval.');
        }
        if (this.masterTimeout < this.nodeTimeout) {
            throw new Error('masterTimeout must be greater than or equal to nodeTimeout.');
        }
        this.bondToAddress = !!options.address;
        const settings = {
            address: options.address || '0.0.0.0',
            port: options.port || 12345,
            key: options.key || null,
            reuseAddr: (options.reuseAddr === false) ? false : true,
            instanceUuid: this.instanceUuid,
            dictionary: (options.dictionary || []).concat(['isMaster', 'isMasterEligible', 'weight', 'prefMode', 'address', 'advertisement']).concat(reservedEvents)
        };
        this.broadcast = new BroadcastNetwork(options.broadcast, settings);
        this.broadcast.on('error', (error) => this.emit('error', error));
        this.broadcast.on('hello', (data, obj, rinfo) => this.onHello(data[0], obj, rinfo, MulticommMode.Broadcast));
        ++settings.port;
        this.multicast = new MulticastNetwork(options.multicast, options.multicastTTL, settings);
        this.multicast.on('error', (error) => this.emit('error', error));
        this.multicast.on('hello', (data, obj, rinfo) => this.onHello(data[0], obj, rinfo, MulticommMode.Multicast));
        settings.port = options.unicastPort || (settings.port + Math.ceil(Math.random() * 100));
        this.dyunicast = new DynamicUnicastNetwork(settings);
        this.dyunicast.on('error', (error) => this.emit('error', error));
        this.dyunicast.on('direct', (data, obj, rinfo) => this.emit('direct', data, obj, rinfo));
        this.me.weight = options.weight || Discover.weight();
        this.me.prefMode = MulticommMode.Broadcast;
        this.me.isMaster = this.me.isMasterEligible = options.isMasterEligible || false;
        this.me.unicastPort = settings.port;
        this.me.advertisement = advertisement;
    }
    static weight() {
        return -(Date.now() / Math.pow(10, String(Date.now()).length));
    }
    start() {
        return new Promise(async (resolve, reject) => {
            if (this.running) {
                resolve(false);
            }
            try {
                await this.broadcast.start();
            }
            catch (err) {
                this.broadcast.stop();
                reject(err);
            }
            try {
                await this.multicast.start();
            }
            catch (err) {
                this.broadcast.stop();
                this.multicast.stop();
                reject(err);
            }
            try {
                await this.dyunicast.start();
            }
            catch (err) {
                this.broadcast.stop();
                this.multicast.stop();
                this.dyunicast.stop();
                reject(err);
            }
            this.running = true;
            this.checkId = setInterval(async () => {
                let mastersFound = 0;
                let higherWeightFound = false;
                let higherWeightMastersFound = 0;
                this.nodes.forEach((node, key) => {
                    const timeout = +new Date() - node.lastSeen;
                    if (timeout > this.nodeTimeout) {
                        if (node.isMaster && timeout < this.masterTimeout) {
                            mastersFound++;
                        }
                        this.nodes.delete(key);
                        this.emit('removed', node);
                    }
                    else if (node.isMaster) {
                        mastersFound++;
                        if (node.weight >= this.me.weight) {
                            higherWeightMastersFound++;
                        }
                    }
                    else if (node.isMasterEligible && node.weight > this.me.weight) {
                        higherWeightFound = true;
                    }
                });
                if (this.me.isMasterEligible && !this.me.isMaster) {
                    if (mastersFound < this.mastersRequired && !higherWeightFound) {
                        await this.promote();
                    }
                }
                else if (this.me.isMaster) {
                    if (mastersFound >= this.mastersRequired && higherWeightMastersFound) {
                        await this.demote(false);
                    }
                }
            }, this.checkInterval);
            this.helloId = setInterval(() => this.hello(), this.helloInterval);
            this.resetId = setInterval(() => {
                if (this.me.prefMode === MulticommMode.Broadcast) {
                    this.broadcastHelloCounter = 1;
                    this.multicastHelloCounter = 0;
                }
                else {
                    this.broadcastHelloCounter = 0;
                    this.multicastHelloCounter = 1;
                }
            }, this.resetInterval);
            resolve(true);
        });
    }
    stop() {
        if (!this.running) {
            return false;
        }
        this.broadcast.stop();
        this.multicast.stop();
        this.dyunicast.stop();
        clearInterval(this.checkId);
        clearInterval(this.helloId);
        clearInterval(this.resetId);
        this.running = false;
    }
    async setAdvertisement(advertisement) {
        this.me.advertisement = advertisement;
        await this.hello();
    }
    async setMasterEligible() {
        this.me.isMasterEligible = true;
        await this.hello();
    }
    async promote() {
        this.me.isMasterEligible = true;
        this.me.isMaster = true;
        this.emit('promotion', this.me);
        await this.hello();
    }
    async demote(permanent) {
        this.me.isMasterEligible = !permanent;
        this.me.isMaster = false;
        this.emit('demotion', this.me);
        await this.hello();
    }
    get isMaster() {
        return this.me.isMaster;
    }
    get id() {
        return this.instanceUuid;
    }
    async hello() {
        let sent = false;
        try {
            await this.broadcast.send('hello', this.me);
            sent = true;
        }
        catch (e) {
            //
        }
        try {
            await this.multicast.send('hello', this.me);
            sent = true;
        }
        catch (e) {
            //
        }
        if (sent) {
            this.emit('helloEmitted');
        }
    }
    eachNode(fn) {
        this.nodes.forEach((node) => fn(node));
    }
    join(channel, fn) {
        if (reservedEvents.includes(channel)) {
            return false;
        }
        if (this.channels.has(channel)) {
            return false;
        }
        if (fn) {
            this.on(channel, fn);
        }
        this.broadcast.on(channel, (data, obj, rinfo) => {
            this.emit(channel, data, obj, rinfo);
        });
        this.multicast.on(channel, (data, obj, rinfo) => {
            this.emit(channel, data, obj, rinfo);
        });
        this.dyunicast.on(channel, (data, obj, rinfo) => {
            this.emit(channel, data, obj, rinfo);
        });
        this.channels.add(channel);
        return true;
    }
    leave(channel) {
        this.broadcast.removeAllListeners(channel);
        this.multicast.removeAllListeners(channel);
        this.dyunicast.removeAllListeners(channel);
        this.channels.delete(channel);
        return true;
    }
    async send(channel, ...obj) {
        if (reservedEvents.includes(channel)) {
            return false;
        }
        const groups = _.groupBy([...this.nodes.values()], (node) => node.preferredMode(this.nodeTimeout));
        const preferBroadcast = groups[MulticommMode.Broadcast] || [];
        const preferMulticast = groups[MulticommMode.Multicast] || [];
        const preferUnicast = groups[MulticommMode.Unicast] || [];
        if (preferBroadcast.length === 0 && preferMulticast.length === 0) {
            await Promise.all(preferUnicast.map((node) => this.dyunicast.sendTo(node.address, node.unicastPort, channel, 0, ...obj)));
        }
        else if (preferBroadcast.length >= preferMulticast.length) {
            await Promise.all([
                this.broadcast.send(channel, ...obj),
                ...preferMulticast.map((node) => this.dyunicast.sendTo(node.address, node.unicastPort, channel, 0, ...obj)),
                ...preferUnicast.map((node) => this.dyunicast.sendTo(node.address, node.unicastPort, channel, 0, ...obj))
            ]);
        }
        else {
            await Promise.all([
                this.multicast.send(channel, ...obj),
                ...preferBroadcast.map((node) => this.dyunicast.sendTo(node.address, node.unicastPort, channel, 0, ...obj)),
                ...preferUnicast.map((node) => this.dyunicast.sendTo(node.address, node.unicastPort, channel, 0, ...obj))
            ]);
        }
        return true;
    }
    async sendTo(id, maxRetries, ...obj) {
        const dest = [...this.nodes.values()].find((node) => node.id === id);
        if (!dest) {
            return false;
        }
        else {
            await this.dyunicast.sendTo(dest.address, dest.unicastPort, 'direct', maxRetries, ...obj);
        }
        return true;
    }
    async onHello(data, obj, rinfo, mode) {
        /*
         * When receiving hello messages we need things to happen in the following order:
         *  - make sure the node is in the node list
         *  - if hello is from new node, emit added
         *  - if hello is from new master and we are master, demote
         *  - if hello is from new master emit master
         *
         * need to be careful not to over-write the old node object before we have information
         * about the old instance to determine if node was previously a master.
         */
        const isNew = !this.nodes.has(obj.iid);
        const node = this.nodes.get(obj.iid) || new Node(!this.bondToAddress);
        const wasMaster = node.isMaster;
        node.id = obj.iid;
        switch (mode) {
            case MulticommMode.Broadcast:
                ++this.broadcastHelloCounter;
                node.lastSeenBroadcast = +new Date();
                break;
            case MulticommMode.Multicast:
                ++this.multicastHelloCounter;
                node.lastSeenMulticast = +new Date();
                break;
        }
        this.me.prefMode = (this.broadcastHelloCounter + 1) >= this.multicastHelloCounter ? MulticommMode.Broadcast : MulticommMode.Multicast;
        node.address = rinfo.address;
        node.unicastPort = data.unicastPort || rinfo.port;
        node.hostName = obj.hostName;
        node.isMaster = data.isMaster;
        node.isMasterEligible = data.isMasterEligible;
        node.weight = data.weight;
        if ('prefMode' in data) {
            node.prefMode = data.prefMode;
        }
        node.advertisement = data.advertisement;
        if (isNew) {
            this.nodes.set(obj.iid, node);
            this.emit('added', node, obj, rinfo);
        }
        this.emit('helloReceived', node);
        if (node.isMaster) {
            if ((isNew || !wasMaster)) {
                let masterCount = (this.me.isMaster) ? 1 : 0;
                this.nodes.forEach((node) => {
                    if (node.isMaster) {
                        masterCount++;
                    }
                });
                if (this.me.isMaster && masterCount > this.mastersRequired && node.weight > this.me.weight) {
                    await this.demote(false);
                }
                this.emit('master', node, obj, rinfo);
            }
        }
    }
}
function isLocalIP(ip) {
    try {
        return _.flatten(_.values(os.networkInterfaces())).filter((iface) => iface.family === 'IPv4' && iface.internal === false).some(({ address }) => address === ip);
    }
    catch (_a) {
        return true;
    }
}
//# sourceMappingURL=discover.js.map