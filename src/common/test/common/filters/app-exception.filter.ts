import { BadRequestException, GatewayTimeoutException, HttpException, HttpStatus, InternalServerErrorException, NotFoundException, RequestTimeoutException } from "@nestjs/common";
import { AppException } from "../../../exception/app.exception";

export const AppExceptionFilter_AppExceptionResultMessage = 'AppExceptionFilter Test';
export const AppExceptionFilter_AppExceptionResult : any = new AppException(AppExceptionFilter_AppExceptionResultMessage,
                                                                AppExceptionFilter_AppExceptionResultMessage, 
                                                                HttpStatus.I_AM_A_TEAPOT, 
                                                                true);

export const AppExceptionFilter_HttpExceptionResultMessage = 'HttpExceptionFilter Test';
export const AppExceptionFilter_HttpExceptionResult : any = new HttpException(AppExceptionFilter_HttpExceptionResultMessage,
                                                                HttpStatus.I_AM_A_TEAPOT);

export const AppExceptionFilter_InternalServerExceptionResultMessage = 'InternalServerExceptionFilter Test';
export const AppExceptionFilter_InternalServerExceptionResult : any = new DOMException(AppExceptionFilter_InternalServerExceptionResultMessage,
                                                                AppExceptionFilter_InternalServerExceptionResultMessage);