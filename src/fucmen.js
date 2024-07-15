import { EventEmitter } from 'events';
import { Discover } from './discover';
export class Fucmen extends EventEmitter {
    constructor(advertisement, discoveryOptions) {
        super();
        this.allNodes = new Map();
        this.discover = new Discover(discoveryOptions, advertisement);
        this.discover.on('added', (node) => this.allNodes.set(node.id, { id: node.id, host: node.address + ':' + node.unicastPort, master: node.isMaster, adv: node.advertisement }));
        this.discover.on('promotion', () => this.emit('promoted'));
        this.discover.on('demotion', () => this.emit('demoted'));
        this.discover.on('master', (node) => this.emit('master', { id: node.id, host: node.address + ':' + node.unicastPort, master: true, adv: node.advertisement }));
        this.discover.on('error', (error) => this.emit('error', error));
        this.discover.on('direct', (data, obj, rinfo) => this.emit('direct', data, obj, rinfo));
        this.restart().then((started) => started && this.emit('ready')).catch((err) => { throw err; });
    }
    get id() {
        return this.discover.id;
    }
    get isMaster() {
        return this.discover.isMaster;
    }
    get nodes() {
        const nodes = [];
        this.allNodes.forEach((node) => nodes.push(node));
        return nodes;
    }
    get connections() {
        const nodes = [];
        this.discover.eachNode((node) => nodes.push({ id: node.id, host: node.address + ':' + node.unicastPort, master: node.isMaster, adv: node.advertisement }));
        return nodes;
    }
    setAdvertisement(advertisement) {
        return this.discover.setAdvertisement(advertisement);
    }
    setMasterEligible() {
        return this.discover.setMasterEligible();
    }
    promote() {
        return this.discover.promote();
    }
    demote(permanent) {
        return this.discover.demote(permanent);
    }
    publish(channel, ...data) {
        return this.discover.send(channel, ...data);
    }
    join(channel, listener, withFrom) {
        if (withFrom) {
            return this.discover.join(channel, (data, obj, rinfo) => listener(this.getNodeFromId(obj.iid), ...data));
        }
        else {
            return this.discover.join(channel, (data, obj, rinfo) => listener(...data));
        }
    }
    leave(channel) {
        return this.discover.leave(channel);
    }
    sendTo(id, reliableMaxRetries, ...data) {
        return this.discover.sendTo(id, reliableMaxRetries === true ? 3 : (reliableMaxRetries || 0), ...data);
    }
    onDirectMessage(listener, withFrom) {
        if (withFrom) {
            this.discover.on('direct', (data, obj, rinfo) => listener(this.getNodeFromId(obj.iid), ...data));
        }
        else {
            this.discover.on('direct', (data, obj, rinfo) => listener(...data));
        }
    }
    restart() {
        this.discover.stop();
        return this.discover.start();
    }
    getNodeFromId(id) {
        return this.connections.find((node) => node.id === id);
    }
}
//# sourceMappingURL=fucmen.js.map