import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from "@nestjs/common";
import * as amqp from "amqplib";
import { Channel, Connection } from "amqplib";

@Injectable()
export class Micro1RabbitMQService implements OnModuleInit, OnModuleDestroy {
    private channel: Channel;
    private connection: Connection;
    private readonly logger = new Logger(Micro1RabbitMQService.name);

    //queues
    private readonly myqueue = 'microservice_1_queue';
    private readonly otherqueue = 'microservice_2_queue';

    async onModuleInit() {
        await this.connectToRabbitMQ();
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

            // create both queues before use
            await this.channel.assertQueue(this.myqueue, { durable: true });
            await this.channel.assertQueue(this.otherqueue, { durable: true });

            this.logger.log("Connected to RabbitMQ and created the channel");
            this.listenForMessages();
        } catch (error) {
            this.logger.error("Error connecting to RabbitMQ:", error);
            process.exit(1);
        }
    }

    async sendMessage(payload: string) {
        try {
            // amqp is binary protocol on tcp so send in binary format
            const messageBuffer = Buffer.from(payload);
            this.channel.sendToQueue(this.otherqueue, messageBuffer, {
                persistent: true,
            });

            this.logger.log(`Message sent to queue '${this.otherqueue}': ${payload}`);
        } catch (error) {
            this.logger.error("Error sending message to RabbitMQ:", error);
        }
    }

    async listenForMessages() {
        try {
            this.channel.consume(
                this.myqueue,
                async (msg: any) => {
                    if (msg) {
                        // convert binary format to string
                        const content = msg.content.toString();
                        await new Promise(resolve => setTimeout(resolve, 5000));

                        this.logger.log(`Received message: ${content}`);
                        this.channel.ack(msg);
                    } else {
                        this.logger.warn("No message received");
                    }
                },
                { noAck: false }
            );
        } catch (error) {
            this.logger.error("Error while listening for messages:", error);
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