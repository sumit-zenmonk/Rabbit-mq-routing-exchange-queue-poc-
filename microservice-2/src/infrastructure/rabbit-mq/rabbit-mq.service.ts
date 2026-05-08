import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from "@nestjs/common";
import * as amqp from "amqplib";
import { Channel, Connection } from "amqplib";
import { ExchangeTypeEnum, XMatchHeaderEnum } from "src/infrastructure/rabbit-mq/enum/rabbit-mq.enum";
import { ExchangeType, PublishHeadersInterface } from "src/infrastructure/rabbit-mq/type/rabbit-mq.type";

@Injectable()
export class Micro2RabbitMQService implements OnModuleInit, OnModuleDestroy {
    private channel: Channel;
    private connection: Connection;
    private readonly logger = new Logger(Micro2RabbitMQService.name);

    //queues
    private readonly micro_two_direct = 'micro_two_direct';
    private readonly micro_two_topic = 'micro_two_topic';
    private readonly micro_two_fanout = 'micro_two_fanout';
    private readonly micro_two_header = 'micro_two_header';

    async onModuleInit() {
        //connect rabbitmq
        await this.connectToRabbitMQ();

        // make some simple testing queues
        await this.setupExchangeQueueAndBind(this.micro_two_direct, 'direct_exchange', 'micro2', ExchangeTypeEnum.DIRECT);
        await this.consumeMessages(this.micro_two_direct);

        await this.setupExchangeQueueAndBind(this.micro_two_topic, 'topic_exchange', 'micro.*', ExchangeTypeEnum.TOPIC);
        await this.consumeMessages(this.micro_two_topic);

        await this.setupExchangeQueueAndBind(this.micro_two_fanout, 'fanout_exchange', '', ExchangeTypeEnum.FANOUT);
        await this.consumeMessages(this.micro_two_fanout);

        await this.setupExchangeQueueAndBind(this.micro_two_header, 'headers_exchange', '', ExchangeTypeEnum.HEADERS, { "x-match": XMatchHeaderEnum.ALL, "topic": "format", "type": "pdf" });
        await this.consumeMessages(this.micro_two_header);
    }

    async onModuleDestroy() {
        await this.closeConnection();
    }

    async connectToRabbitMQ() {
        try {
            // create connection then i can create multiple channels
            this.connection = await amqp.connect(process.env.RABBIT_MQ_URL ?? "amqp://localhost:5672");
            this.channel = await this.connection.createChannel();

            // fair dispatch
            this.channel.prefetch(1);

            // checking channel connection
            this.channel.on('error', (err: any) => {
                this.logger.error('Channel error', err);
            });

            // reconnect if connection closed
            this.connection.on("close", () => {
                this.logger.warn("Connection closed, reconnecting...");
                setTimeout(() => this.connectToRabbitMQ(), 1000);
            });

            this.logger.log("Connected to RabbitMQ and created the channel");
        } catch (error) {
            this.logger.error("Error connecting to RabbitMQ:", error);
        }
    }

    // inset exchange + inset queue -> bind both
    async setupExchangeQueueAndBind(
        queue: string,
        exchange: string,
        routingKey: string,
        type: ExchangeType = ExchangeTypeEnum.DIRECT,
        headers?: PublishHeadersInterface
    ) {
        try {
            // ensure exchange + queue
            await this.channel.assertExchange(exchange, type, { durable: true, });
            await this.channel.assertQueue(queue, { durable: true });

            // bind queue to exchange
            await this.channel.bindQueue(queue, exchange, routingKey, headers);
        } catch (error) {
            this.logger.error("Error while setting up queue:", error);
        }
    }

    // consume messages
    async consumeMessages(queue: string) {
        try {
            this.channel.consume(
                queue,
                async (msg: any) => {
                    if (!msg) return;

                    try {
                        // parse message (binary to string)
                        const content = JSON.parse(msg.content.toString());

                        // processing
                        await new Promise(res => setTimeout(res, 10000));
                        this.logger.log(`Received => ${queue}: ${JSON.stringify(content)}`);

                        // i snet acknowledge
                        this.channel.ack(msg);
                    } catch (err) {
                        this.logger.error(`Consuming queue = ${queue} error:`, err);
                        this.channel.nack(msg, false, false); // drop message from queue if setuped dlq then push into that
                        // this.channel.nack(msg, false, true); // requeue message in queue
                    }
                },
                { noAck: false }
            );
        } catch (error) {
            this.logger.error("Error while consuming messages:", error);
        }
    }

    // send message using exchange
    async publishToExchange(
        exchange: string,
        routingKey: string,
        message: any,
        // type: ExchangeType = ExchangeTypeEnum.DIRECT,
        headers?: PublishHeadersInterface
    ) {
        try {
            // ensure exchange exists
            // await this.channel.assertExchange(exchange, type, { durable: true, });

            // amqp is binary protocol on tcp so send in binary format
            const buffer = Buffer.from(JSON.stringify(message));

            // publish message
            this.channel.publish(exchange, routingKey, buffer, {
                persistent: true,
                headers: headers
            });

            this.logger.log(`Sent => exchange = ${exchange} | key = ${routingKey}`);
        } catch (error) {
            this.logger.error("Send error:", error);
        }
    }

    async closeConnection() {
        try {
            // close channel + connection
            await this.channel?.close();
            await this.connection?.close();

            this.logger.log("RabbitMQ connection closed");
        } catch (error) {
            this.logger.error("Error closing RabbitMQ connection:", error);
        }
    }
}