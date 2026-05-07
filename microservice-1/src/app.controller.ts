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
  async send(@Body() body: { message: string }) {
    if (!body.message) {
      throw new BadRequestException("message required")
    }

    await this.micro1RabbitMQService.publishToExchange('direct_exchange', 'micro1', { message: body.message }, ExchangeTypeEnum.DIRECT);
    return { status: "Message sent (direct)" };
  }

  // fanout exchange api
  @Post("broadcast")
  async broadcast(@Body() body: { message: string }) {
    if (!body.message) {
      throw new BadRequestException("message required")
    }

    await this.micro1RabbitMQService.publishToExchange('fanout_exchange', '', { message: body.message }, ExchangeTypeEnum.FANOUT);
    return { status: "Broadcast sent" };
  }

  // topic exchange api
  @Post("topic")
  async topic(@Body() body: { message: string; key: string }) {
    if (!body.message || !body.key) {
      throw new BadRequestException("key , message required")
    }

    await this.micro1RabbitMQService.publishToExchange('topic_exchange', body.key, { message: body.message }, ExchangeTypeEnum.TOPIC);
    return { status: "Topic message sent" };
  }

  // headers exchange api
  @Post("headers")
  async headers(@Req() req: Request, @Body() body: { message: string }) {
    if (!body.message) {
      throw new BadRequestException("message required")
    }

    await this.micro1RabbitMQService.publishToExchange('headers_exchange', '', { message: body.message }, ExchangeTypeEnum.HEADERS, { "x-match": XMatchHeaderEnum.ALL, ...req.headers });
    return { status: "headers message sent" };
  }
}
