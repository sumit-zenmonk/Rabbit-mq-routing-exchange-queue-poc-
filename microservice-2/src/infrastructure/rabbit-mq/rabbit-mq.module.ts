import { Global, Module } from '@nestjs/common';
import { Micro2RabbitMQService } from './rabbit-mq.service';

@Global()
@Module({
    providers: [Micro2RabbitMQService],
    exports: [Micro2RabbitMQService]
})
export class RabbitMQModule { }