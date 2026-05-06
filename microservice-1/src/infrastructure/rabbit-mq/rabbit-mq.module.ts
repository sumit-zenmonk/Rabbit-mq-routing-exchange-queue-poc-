import { Global, Module } from '@nestjs/common';
import { Micro1RabbitMQService } from './rabbit-mq.service';

@Global()
@Module({
    providers: [Micro1RabbitMQService],
    exports: [Micro1RabbitMQService]
})
export class RabbitMQModule { }