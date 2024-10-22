import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import { AppException } from '../../common/exception/app.exception';

@Catch()
export class AppExceptionFilter implements ExceptionFilter {
  catch(exception: Error, host: ArgumentsHost) {
    Logger.error(exception);
    const ctx = host.switchToHttp();
    const request = ctx.getRequest();
    const response = ctx.getResponse();

    let untrustedException = false;
    if (exception instanceof AppException) {
      const appException = exception as AppException;
      response.status(appException.getStatus()).json({
        statusCode: appException.getStatus(),
        message: appException.externalMessage,
      });
      untrustedException = !appException.isOperational;
    } else if (exception instanceof HttpException) {
      response.status(exception.getStatus()).json({
        statusCode: exception.getStatus(),
        message: exception.message,
      });
    } else {
      response.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
        statusCode: HttpStatus.INTERNAL_SERVER_ERROR,
        message: 'Internal Server Error',
      });
      untrustedException = true;
    }

    if (untrustedException) {
      Logger.error(`Untrusted Exception, Nest Process will exit`);
      process.exit(1);
    }
  }
}
