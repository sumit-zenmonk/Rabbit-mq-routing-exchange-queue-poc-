import { BadRequestException, Body, Controller, Get, Post, Req } from '@nestjs/common';
import { AppService } from './app.service';
import { Micro2RabbitMQService } from './infrastructure/rabbit-mq/rabbit-mq.service';
import type { Request } from 'express';
import { XMatchHeaderEnum } from './infrastructure/rabbit-mq/enum/rabbit-mq.enum';

@Controller()
export class AppController {
  constructor(
    private readonly appService: AppService,
    private readonly micro2RabbitMQService: Micro2RabbitMQService
  ) { }

  @Get()
  getHello(): string {
    return this.appService.getHello();
  }

  // direct exchange api
  @Post("direct")
  async send(@Body() body: any) {
    await this.micro2RabbitMQService.publishToExchange('direct_exchange', body.key || 'micro1', body);
    return { status: "Message sent (direct)" };
  }

  // fanout exchange api
  @Post("broadcast")
  async broadcast(@Body() body: any) {
    await this.micro2RabbitMQService.publishToExchange('fanout_exchange', '', body);
    return { status: "Broadcast sent" };
  }

  // topic exchange api
  @Post("topic")
  async topic(@Body() body: any) {
    if (!body.key) {
      throw new BadRequestException("key required")
    }

    await this.micro2RabbitMQService.publishToExchange('topic_exchange', body.key, body);
    return { status: "Topic message sent" };
  }

  // headers exchange api
  @Post("headers")
  async headers(@Req() req: Request, @Body() body: any) {
    const headers = { type: req.headers.type, topic: req.headers.topic };
    await this.micro2RabbitMQService.publishToExchange('headers_exchange', '', body, { "x-match": XMatchHeaderEnum.ALL, ...headers });
    return { status: "headers message sent" };
  }
}
