import { BadRequestException, Body, Controller, Get, Post } from '@nestjs/common';
import { AppService } from './app.service';
import { Micro1RabbitMQService } from './infrastructure/rabbit-mq/rabbit-mq.service';

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

  @Post("send")
  async sendMessage(@Body() payload: { message: string }) {
    await this.micro1RabbitMQService.sendMessage(payload.message);
    return { status: "Message sent successfully" };
  }

}
