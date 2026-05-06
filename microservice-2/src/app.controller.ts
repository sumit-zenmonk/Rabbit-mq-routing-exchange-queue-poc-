import { BadRequestException, Body, Controller, Get, Post } from '@nestjs/common';
import { AppService } from './app.service';
import { Micro2RabbitMQService } from './infrastructure/rabbit-mq/rabbit-mq.service';

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

  @Post("send")
  async sendMessage(@Body() payload: { message: string }) {
    await this.micro2RabbitMQService.sendMessage(payload.message);
    return { status: "Message sent successfully" };
  }

}
