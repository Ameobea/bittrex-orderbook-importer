const net = require('net');
const PORT = 9001;
const HOST = 'localhost';

import { DBUpdate } from './typings';

interface TectonicResponse {
    success: boolean;
    data: string;
}

type SocketMsgCb = (res: TectonicResponse) => void;

interface SocketQuery {
    message: string;
    cb: SocketMsgCb;
    onError: (err: any) => void;
}

class TectonicDB {

    port : number;
    address : string;
    socket: any;
    private socketSendQueue: SocketQuery[];
    private activeQuery?: SocketQuery;
    private initialized: boolean;

    constructor(port=PORT, address=HOST) {
        this.socket = new net.Socket();
        this.activeQuery = null;
        this.address = address || HOST;
        this.port = port || PORT;
        this.initialized = false;
        this.init();
    }

    async init() {
        const client = this;

        client.socketSendQueue = [];

        client.socket.connect(client.port, client.address, () => {
            console.log(`Tectonic client connected to: ${client.address}:${client.port}`);
            this.initialized = true;

            // process any queued queries
            if(this.socketSendQueue.length > 0) {
                console.log('Sending queued message after DB connected...');
                client.activeQuery = this.socketSendQueue.shift();
                client.sendSocketMsg(this.activeQuery.message);
            }
        });

        client.socket.on('close', () => {
            console.log('Client closed');
        });

        client.socket.on('data', (data: any) =>
            this.handleSocketData(data));

        client.socket.on('error', (err: any) => {
            if(client.activeQuery) {
                client.activeQuery.onError(err);
            }
        });
    }

    async info() {
        return await this.cmd('INFO');
    }

    async ping() {
        return await this.cmd('PING');
    }

    async help() {
        await this.cmd('HELP');
    }

    async add(update : DBUpdate) {
        const { timestamp, seq, is_trade, is_bid, price, size } = update;
        return await this.cmd(`ADD ${timestamp}, ${seq}, ${is_trade ? 't' : 'f'}, ${is_bid ? 't':'f'}, ${price}, ${size};`);
    }

    async bulkadd(updates : DBUpdate[]) {
        await this.cmd('BULKADD');
        for (const { timestamp, seq, is_trade, is_bid, price, size} of updates) {
            await this.cmd(`${timestamp}, ${seq}, ${is_trade ? 't' : 'f'}, ${is_bid ? 't':'f'}, ${price}, ${size};`);
        }
        return await this.cmd('DDAKLUB');
    }

    async bulkadd_into(updates : DBUpdate[], db: string) {
        await this.cmd('BULKADD INTO ' + db);
        for (const { timestamp, seq, is_trade, is_bid, price, size} of updates) {
            await this.cmd(`${timestamp}, ${seq}, ${is_trade ? 't' : 'f'}, ${is_bid ? 't':'f'}, ${price}, ${size};`);
        }
        return await this.cmd('DDAKLUB');
    }

    async insert(update: DBUpdate, db : string) {
        const { timestamp, seq, is_trade, is_bid, price, size } = update;
        return await this.cmd(`ADD ${timestamp}, ${seq}, ${is_trade ? 't' : 'f'}, ${is_bid ? 't':'f'}, ${price}, ${size}; INTO ${db}`);
    }

    async getall() {
        const {success, data} = await this.cmd('GET ALL AS JSON');
        console.log(data);
        if (success) {
            return JSON.parse(data);
        } else {
            return null;
        }
    }

    async get(n : number) {
        const {success, data} = await this.cmd(`GET ${n} AS JSON`);
        if (success) {
            return JSON.parse(data);
        } else {
            return data;
        }
    }

    async clear() {
        return await this.cmd('CLEAR');
    }

    async clearall() {
        return await this.cmd('CLEAR ALL');
    }

    async flush() {
        return await this.cmd('FLUSH');
    }

    async flushall() {
        return await this.cmd('FLUSH ALL');
    }

    async create(dbname: string) {
        console.log('CREATING');
        return await this.cmd(`CREATE ${dbname}`);
    }

    async use(dbname: string) {
        return await this.cmd(`USE ${dbname}`);
    }

    handleSocketData(data: any) {
        const client = this;

        const success = data.subarray(0, 8)[0] === 1;
        const len = new Uint32Array(data.subarray(8,9))[0];
        const dataBody : string = String.fromCharCode.apply(null, data.subarray(9, len+12));
        const response : TectonicResponse = {success, data: dataBody};
        console.log('RESPONSE: ', response);

        if (client.activeQuery) {
            // execute the stored callback with the result of the query, fulfilling the promise
            client.activeQuery.cb(response);
        }

        if (data.toString().endsWith('exit')) {
            client.exit();
        }

        // if there's something left in the queue to process, do it next
        // otherwise set the current query to empty
        if(client.socketSendQueue.length === 0) {
            client.activeQuery = null;
        } else {
            // equivalent to `popFront()`
            client.activeQuery = this.socketSendQueue.shift();
        }
    }

    sendSocketMsg(msg: string) {
        this.socket.write(msg+'\n');
    }

    cmd(message: string) : Promise<TectonicResponse> {
        const client = this;
        return new Promise((resolve, reject) => {
            const query: SocketQuery = {
                message,
                cb: resolve,
                onError: reject,
            };

            if(!client.activeQuery || !this.initialized) {
                // socket is idle or not yet ready, so just send the message directly and set the cb
                console.log('Pushing into queue: ' + query.message);
                client.activeQuery = query;
                client.sendSocketMsg(message);
            } else {
                console.log('PUSHING ' + query.message);
                // push message into the queue to be processed after the others
                client.socketSendQueue.push(query);
            }
        });
    }

    exit() {
        this.socket.destroy();
    }
}

// module.exports = TectonicDB;

export default TectonicDB;
