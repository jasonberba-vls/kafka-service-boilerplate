import { Body, Controller, Get, Param, Post, Put, Query } from '@nestjs/common';

@Controller('health')
export class HealthController {
  constructor() {}

  @Get()
  async check() {
    return 'ok';
  }
}
