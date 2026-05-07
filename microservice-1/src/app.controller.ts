import { BadRequestException, Body, Controller, Get, Post, Req } from '@nestjs/common';
import { AppService } from './app.service';
import { Micro1RabbitMQService } from './infrastructure/rabbit-mq/rabbit-mq.service';
import type { Request } from 'express';
import { ExchangeTypeEnum, XMatchHeaderEnum } from './domain/rabbit-mq/enum/rabbit-mq.enum';

@Controller()
export class AppController {
  constructor(
    private readonly appService: AppService,
    private readonly micro1RabbitMQService: Micro1RabbitMQService
  ) { }

  @Get()
  getHello(): string {
    return this.appService.getHello();
  }

  // direct exchange api
  @Post("direct")
  async send(@Body() body: any) {
    await this.micro1RabbitMQService.publishToExchange('direct_exchange', body.key || 'micro1', body, ExchangeTypeEnum.DIRECT);
    return { status: "Message sent (direct)" };
  }

  // fanout exchange api
  @Post("broadcast")
  async broadcast(@Body() body: any) {
    await this.micro1RabbitMQService.publishToExchange('fanout_exchange', '', body, ExchangeTypeEnum.FANOUT);
    return { status: "Broadcast sent" };
  }

  // topic exchange api
  @Post("topic")
  async topic(@Body() body: any) {
    if (!body.key) {
      throw new BadRequestException("key required")
    }

    await this.micro1RabbitMQService.publishToExchange('topic_exchange', body.key, body, ExchangeTypeEnum.TOPIC);
    return { status: "Topic message sent" };
  }

  // headers exchange api
  @Post("headers")
  async headers(@Req() req: Request, @Body() body: any) {
    const headers = { type: req.headers.type, topic: req.headers.topic };
    await this.micro1RabbitMQService.publishToExchange('headers_exchange', '', body, ExchangeTypeEnum.HEADERS, { "x-match": XMatchHeaderEnum.ALL, ...headers });
    return { status: "headers message sent" };
  }
}
