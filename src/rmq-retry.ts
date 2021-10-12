import amqp, { ChannelWrapper } from "amqp-connection-manager";
import { Channel, ConsumeMessage } from "amqplib";

const url = 'amqp://localhost:5672';
const tlExchange = 'TLX_send_email';
const dlExchange = 'DLX_send_email';
const serviceQueue = 'send_email';
const time10sQueue = 'TTL-Retry-10S';
const time30sQueue = 'TTL-Retry-30S';
const time50sQueue = 'TTL-Retry-50S';
const time10bindPattern = 'Retry-once';
const time30bindPattern = 'Retry-twice';
const time50bindPattern = 'Retry-thrice';
const ONE_SEC = 1000;

const connection = amqp.connect([url]);

function queueConsumer(msg: ConsumeMessage, channel: Channel) {
    try {
        const messageJSON = JSON.parse(msg.content.toString());
        if(channel && messageJSON.data.payload.email !== 'antoniel15975@hotmail.com') {
            throw new Error('Error');
        } else {
            channel.ack(msg);
            console.log(`Mail send to: ${messageJSON.data.payload.email}`);
        }
    } catch (error) {
        if(channel) {
            const messageJSON = JSON.parse(msg.content.toString());

            if(messageJSON.retrys) {
                messageJSON.retrys += 1;
            } else {
                messageJSON.retrys = 1;
            }

            const messageBuffer = Buffer.from(JSON.stringify(messageJSON));

            // if(data.fields.routingKey !== "Retry-1" && data.fields.routingKey !== "Retry-2"  && data.fields.routingKey !== "Retry-3" ){
                channel.ack(msg);
            // }

            if(messageJSON.retrys === 1) {
                channel.publish(tlExchange,time10bindPattern, messageBuffer);
                console.log("First attempt at  " + new Date().toISOString());
            }
            if(messageJSON.retrys === 2) {
                channel.publish(tlExchange,time30bindPattern, messageBuffer);
                console.log("Second attempt at  " + new Date().toISOString());
            }
            if(messageJSON.retrys === 3) {
                channel.publish(tlExchange,time50bindPattern, messageBuffer);
                console.log("Third attempt at  " + new Date().toISOString());
            }
            if(messageJSON.retrys > 3) {
                console.log("All the attempts are exceeded hence discarding the message");
            }
        }
    }
}

function init(): ChannelWrapper {
    connection.on("connect", function() {
        console.log("Message broker events for charger is connected!");
    });

    connection.on("close", function() {
        console.log("Message broker events for charger is closed by .close() code call");
    });

    connection.on("disconnect", function(error: Error) {
        console.log("Message broker events for charger is disconnected! Error: ", error);
    });

    return connection.createChannel({
        json: true,
        setup: async (channel: Channel) => {    
            return Promise.all([
                // create exchangers
                channel.assertExchange(dlExchange,'fanout', { durable: true }),
                channel.assertExchange(tlExchange,'direct', { durable: true }),
                // create queues
                channel.assertQueue(serviceQueue, { durable: true }),
                channel.assertQueue(time10sQueue,{ durable: true, deadLetterExchange: dlExchange, messageTtl: 10 * ONE_SEC}),
                channel.assertQueue(time30sQueue,{ durable: true, deadLetterExchange: dlExchange, messageTtl: 30 * ONE_SEC}),
                channel.assertQueue(time50sQueue,{ durable: true, deadLetterExchange: dlExchange, messageTtl: 50 * ONE_SEC}),
                // bind queues and exchangers
                channel.bindQueue(serviceQueue, dlExchange, ''),
                channel.bindQueue(time10sQueue,tlExchange,time10bindPattern),
                channel.bindQueue(time30sQueue,tlExchange,time30bindPattern),
                channel.bindQueue(time50sQueue,tlExchange,time50bindPattern),
                // set cosummer
                channel.consume(serviceQueue, (msg: ConsumeMessage | null) => msg && queueConsumer(msg, channel) )
            ])
        }
    });
}

async function close() {
    await connection.close();
}

init();
export { init, close };

