import { HttpException, HttpStatus } from '@nestjs/common';

export class AppException extends HttpException {
  public readonly internalMessage: string;
  public readonly externalMessage: string;
  public readonly httpStatus: HttpStatus;
  public readonly isOperational: boolean;

  constructor(
    internalMessage: any,
    externalMessage: any,
    httpStatus: HttpStatus,
    isOperational: boolean,
  ) {
    super(externalMessage, httpStatus);
    this.internalMessage = internalMessage;
    this.externalMessage = externalMessage;
    this.httpStatus = httpStatus;
    this.isOperational = isOperational;
  }
}
