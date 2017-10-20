const bittrex = require('./node.bittrex.api');

import {
    DBUpdate,
    ExchangeCallback,

    ExchangeState,
    ExchangeStateUpdate,

    PairUpdate,
    SummaryCallback,

    SummaryState,
} from './typings';

import {
    createTableForPair,
    db,
    tableExistsForPair,
} from './db';

import { toPair } from './utils';

function allMarkets() : Promise<[string]> {
    return new Promise((resolve, reject) => {
        bittrex.getmarketsummaries((data : any, err : never) => {
            if (err) {
                reject(err);
            }
            const ret = data.result.map((market : PairUpdate) => market.MarketName);
            resolve(ret);
        });
    });
}

function formatUpdate(v : ExchangeStateUpdate) {
    const updates : DBUpdate[] = [];

    const pair = toPair(v.MarketName);
    const seq = v.Nounce;
    const timestamp = Date.now() / 1000;

    v.Buys.forEach((buy) => {
        updates.push(
            {
                pair,
                seq,
                is_trade: false,
                is_bid: true,
                price: buy.Rate,
                size: buy.Quantity,
                timestamp,
                type: buy.Type,
            },
        );
    });

    v.Sells.forEach((sell) => {
        updates.push(
            {
                pair,
                seq,
                is_trade: false,
                is_bid: false,
                price: sell.Rate,
                size: sell.Quantity,
                timestamp,
                type: sell.Type,
            },
        );
    });

    v.Fills.forEach((fill) => {
        updates.push(
            {
                pair,
                seq,
                is_trade: true,
                is_bid: fill.OrderType === 'BUY',
                price: fill.Rate,
                size: fill.Quantity,
                timestamp: (new Date(fill.TimeStamp)).getTime() / 1000,
                type: null,
            },
        );
    });

    return updates;
}

function listen(markets : string[], exchangeCallback?: ExchangeCallback, summaryCallback?: SummaryCallback) : void {
    const websocketsclient = bittrex.websockets.subscribe(markets, (data : ExchangeState | SummaryState ) => {
        if (data.M === 'updateExchangeState') {
            data.A.forEach(exchangeCallback);
        } else if (data.M === 'updateSummaryState') {
            data.A[0].Deltas.forEach(summaryCallback);
        } else {
            console.log('--------------',data); // <never>
        }
    });
}

async function initTables(markets : string[]) {
    const pairs = markets.map(toPair);

    const create = await Promise.all(
        pairs.map((pair) => new Promise(async (resolve, reject) => {
            const exists = await tableExistsForPair(pair);
            if (!exists) {
                console.log(`${pair} table does not exist. Creating...`);
                await createTableForPair(pair);
            }
            resolve(true);
        })),
    );

    console.log('Double checking...');
    const created = await Promise.all(pairs.map(tableExistsForPair));
    for (let i = 0; i < created.length; i++) {
        if (!created[i]) {
            throw new Error(`Table for '${pairs[i]}' cannot be created.`);
        }
    }
}

async function watch() {
    try {
        const mkts = ['BTC-NEO', 'BTC-ETH'];
        // let mkts = await allMarkets();
        console.log(mkts);
        await initTables(mkts);
        console.log('Tables created.');
        listen(mkts, (v, i, a) => {
            const updates : DBUpdate[] = formatUpdate(v);
            db.bulkadd_into(updates, updates[0].pair);
        });

    } catch (e) {
        console.log(e);
        throw e;
    }
}

const main = watch;

main();
// test();

function test() {
    db.bulkadd_into([{
        pair: 'default',
        seq: 0,
        is_bid: true,
        is_trade: true,
        size: 0.1,
        price: 0.1,
        timestamp: 100,
        type: 0,
    }], 'default').then(db.exit);
}
