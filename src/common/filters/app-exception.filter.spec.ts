import { Test, TestingModule } from '@nestjs/testing';
import * as appExceptionFilterTestObjects from '../../common/test/common/filters/app-exception.filter';
import { AppExceptionFilter } from '../../common/filters/app-exception.filter';
import { HttpStatus } from '@nestjs/common';
import { Logger } from '@nestjs/common';

describe('AppExceptionFilter', () => {
  //Initialize Objects and Mocks
  let appExceptionFilter: AppExceptionFilter;
  
  //Initialize  Env Object
  const originalEnv = process.env;

  beforeEach(async () => {
    //Initialize Env Key Values
    jest.resetModules();
      process.env = {
        REQUEST_API_TOKEN: 'token_123',
        INTERNAL_API_URL: 'https://intertalapiurl.fake.url'
      };

    const module: TestingModule = await Test.createTestingModule({
    providers: [
        AppExceptionFilter
    ],
    }).compile();

    //Initialize Object to Test
    appExceptionFilter = module.get<AppExceptionFilter>(AppExceptionFilter);
    jest.spyOn(Logger, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
  });
  
  describe('Initialization', () => {
    test('Success', () => {
        //Assert Test Scenarios
        expect(appExceptionFilter).toBeDefined();
    });
  });

  describe('catch', () => {
    test('Success - AppException', async () => {
        let mockJson = jest.fn();
        let mockStatus = jest.fn().mockImplementation(() => ({
            json: mockJson
        }));
        let mockGetResponse = jest.fn().mockImplementation(() => ({
            status: mockStatus
        }));
        let mockHttpArgumentsHost = jest.fn().mockImplementation(() => ({
            getResponse: mockGetResponse,
            getRequest: jest.fn()
        }));
        let  mockArgumentsHost = {
            switchToHttp: mockHttpArgumentsHost,
            getArgByIndex: jest.fn(),
            getArgs: jest.fn(),
            getType: jest.fn(),
            switchToRpc: jest.fn(),
            switchToWs: jest.fn()
        };

        let appExceptionResult: any = {};
        appExceptionResult = appExceptionFilter.catch(appExceptionFilterTestObjects.AppExceptionFilter_AppExceptionResult, mockArgumentsHost);

        //Assert Test Scenarios
        expect(mockStatus).toBeCalledWith(HttpStatus.I_AM_A_TEAPOT);
        expect(mockJson).toBeCalledWith({statusCode: HttpStatus.I_AM_A_TEAPOT, message: appExceptionFilterTestObjects.AppExceptionFilter_AppExceptionResultMessage});
    });

    test('Success - HttpException', async () => {
      let mockJson = jest.fn();
      let mockStatus = jest.fn().mockImplementation(() => ({
          json: mockJson
      }));
      let mockGetResponse = jest.fn().mockImplementation(() => ({
          status: mockStatus
      }));
      let mockHttpArgumentsHost = jest.fn().mockImplementation(() => ({
          getResponse: mockGetResponse,
          getRequest: jest.fn()
      }));
      let  mockArgumentsHost = {
          switchToHttp: mockHttpArgumentsHost,
          getArgByIndex: jest.fn(),
          getArgs: jest.fn(),
          getType: jest.fn(),
          switchToRpc: jest.fn(),
          switchToWs: jest.fn()
      };

      let httpExceptionResult: any = {};
      httpExceptionResult = appExceptionFilter.catch(appExceptionFilterTestObjects.AppExceptionFilter_HttpExceptionResult, mockArgumentsHost);

      //Assert Test Scenarios
      expect(mockStatus).toBeCalledWith(HttpStatus.I_AM_A_TEAPOT);
      expect(mockJson).toBeCalledWith({statusCode: HttpStatus.I_AM_A_TEAPOT, message: appExceptionFilterTestObjects.AppExceptionFilter_HttpExceptionResultMessage});
    });

    test('Success - Internal Server Error', async () => {
      let mockJson = jest.fn();
      let mockStatus = jest.fn().mockImplementation(() => ({
          json: mockJson
      }));
      let mockGetResponse = jest.fn().mockImplementation(() => ({
          status: mockStatus
      }));
      let mockHttpArgumentsHost = jest.fn().mockImplementation(() => ({
          getResponse: mockGetResponse,
          getRequest: jest.fn()
      }));
      const mockExit = jest.fn().mockImplementation(() => ({

      }));

      let  mockArgumentsHost = {
          switchToHttp: mockHttpArgumentsHost,
          getArgByIndex: jest.fn(),
          getArgs: jest.fn(),
          getType: jest.fn(),
          switchToRpc: jest.fn(),
          switchToWs: jest.fn()
      };

      //Handle process.exit
      jest.spyOn(process, 'exit').mockImplementation((code?: number) => undefined as never);

      let internalServerExceptionResult: any = {};
      internalServerExceptionResult = appExceptionFilter.catch(appExceptionFilterTestObjects.AppExceptionFilter_InternalServerExceptionResult, mockArgumentsHost);

      //Assert Test Scenarios
      expect(mockStatus).toBeCalledWith(HttpStatus.INTERNAL_SERVER_ERROR);
      expect(mockJson).toBeCalledWith({statusCode: HttpStatus.INTERNAL_SERVER_ERROR, message: 'Internal Server Error'});
    });
  });
});