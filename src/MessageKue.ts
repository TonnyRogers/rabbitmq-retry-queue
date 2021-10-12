import amqp, { AmqpConnectionManager, ChannelWrapper } from "amqp-connection-manager";
import { Channel, ConsumeMessage } from 'amqplib';

const tlExchange = 'TLX_MyServiceQueue';
const dlExchange = 'DLX_MyServiceQueue';
const serviceQueue = 'MyServiceQueue';
const time10sQueue = 'TTL-Retry-10S';
const time30sQueue = 'TTL-Retry-30S';
const time50sQueue = 'TTL-Retry-50S';
const time10bindPattern = 'Retry-once';
const time30bindPattern = 'Retry-twice';
const time50bindPattern = 'Retry-thrice';

class MessageQ {

    MessageConnector: AmqpConnectionManager;
    MessageChannel: ChannelWrapper | undefined;

    private url = 'amqp://localhost:5672';
    private options = {
        contentEncoding: "utf8",
        contentType: "application/json",
        persistent: true,
    };

    constructor() {
        this.MessageConnector = amqp.connect([this.url], { findServers: () => this.url });
        this.MessageConnector.on('connect', () => console.log('Message broker connected'));
        this.MessageConnector.on('disconnect', (error) => console.log(`Message broker disconnected due ${error.err.stack}`));
    }

    public async init() {
        this.MessageChannel = await this.createChannel(this.MessageConnector);
        this.addChannelEventListeners();
    }

    private async createChannel(MessageConnector: AmqpConnectionManager) {
        const self = this;
        return MessageConnector.createChannel({
            json: true,
            setup: async (channel: Channel) => {
                return Promise.all([
                    channel.assertExchange('events', 'topic',{ durable: true }),
                    channel.assertExchange(dlExchange, 'fanout', {durable: true }),
                    channel.assertExchange(tlExchange, 'direct', {durable: true}),
                    channel.assertQueue(serviceQueue, {durable: true}),
                    channel.assertQueue(time10sQueue, {durable: true , deadLetterExchange: dlExchange, messageTtl: 10000}),
                    channel.assertQueue(time30sQueue, {durable: true , deadLetterExchange: dlExchange, messageTtl: 30000}),
                    channel.assertQueue(time50sQueue, {durable: true , deadLetterExchange: dlExchange, messageTtl: 50000}),
                    channel.bindQueue(serviceQueue, "events", "event.user.#"),
                    channel.bindQueue(serviceQueue, dlExchange, ''),
                    channel.bindQueue(time10sQueue,tlExchange,time10bindPattern),
                    channel.bindQueue(time30sQueue,tlExchange,time30bindPattern),
                    channel.bindQueue(time50sQueue,tlExchange,time50bindPattern),
                    channel.consume(serviceQueue, (msg: ConsumeMessage | null) => msg && self.onUserEvent(msg)), 
                ])
            }
        })
    }

    private addChannelEventListeners() {
        if (this.MessageChannel) {
            this.MessageChannel.on("connect", function() {
            console.log("Message broker events for charger is connected!");
            });
            this.MessageChannel.on("close", function() {
            console.log("Message broker events for charger is closed by .close() code call");
            });
            this.MessageChannel.on("disconnect", function(error: Error) {
            console.log("Message broker events for charger is disconnected! Error: ", error);
            });
        }
    }

    private onUserEvent(data: ConsumeMessage){
        try {
            console.log(data.fields.routingKey + " key received at "+ new Date().toUTCString());
            if(this.MessageChannel){
                // throw error to test retry mechanisms 
                throw new Error('Error');
            }
        }
        catch(error){
            if(this.MessageChannel){

                const messageJSON = JSON.parse(data.content.toString());
                
                console.log(messageJSON);
                
                if(messageJSON.retrys) {
                    messageJSON.retrys += 1;
                } else {
                    messageJSON.retrys = 1;
                }

                // if(data.fields.routingKey !== "Retry-1" && data.fields.routingKey !== "Retry-2"  && data.fields.routingKey !== "Retry-3" ){
                    this.MessageChannel.ack(data);                    
                // }
                
                if(messageJSON.retrys === 1){
                    this.MessageChannel.publish(tlExchange,time10bindPattern,messageJSON)
                    console.log("First attempt at  " + new Date().toUTCString());
                }
                if(messageJSON.retrys === 2){
                    this.MessageChannel.publish(tlExchange,time30bindPattern,messageJSON)
                    console.log("Second attempt at  " + new Date().toUTCString());
                }
                if(messageJSON.retrys === 3){
                    this.MessageChannel.publish(tlExchange,time50bindPattern,messageJSON)
                    console.log("third attept at  " + new Date().toUTCString());
                } 
                if(messageJSON.retrys > 3){
                    console.log("All the attempts are exceeded hence discarding the message");
                }
                return ;
            }
            return ;
        }
    }
}

const messageQ = new MessageQ();
export default messageQ;