import { Test, TestingModule } from '@nestjs/testing';
import { CustomLoggerService } from '../../common/logger/custom-logger.service';
import { ConfigService } from "@nestjs/config";
import rTracer from 'cls-rtracer';
import { Logger } from 'winston';

describe('CustomLoggerService', () => {
  //Initialize Objects and Mocks
  let customLoggerService: CustomLoggerService;
  let testTracerId = '123456';
  
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
        CustomLoggerService,
        ConfigService,
        Logger
    ],
    }).compile();

    //Initialize Object to Test
    customLoggerService = module.get<CustomLoggerService>(CustomLoggerService);
    customLoggerService.logger = module.get<Logger>(Logger);

    //Map response to Mock Objects
    jest.spyOn(rTracer, 'id').mockImplementation(() => testTracerId);
    customLoggerService.logger.info = jest.fn();
    customLoggerService.logger.error = jest.fn();
    customLoggerService.logger.log = jest.fn();
    customLoggerService.logger.debug = jest.fn();
  });

  afterEach(() => {
    process.env = originalEnv;
  });
  
  describe('Initialization', () => {
    test('Success', () => {
        //Assert Test Scenarios
        expect(customLoggerService).toBeDefined();
    });
  });

  describe('log', () => {
    test('Successful - Scenario 1', async () => {
        let logMessage = 'testLogMessage'
        let logResult: any = {};
        logResult = await customLoggerService.log(logMessage);
        //console.log('log Success - Scenario1', logResult)

        //Assert Test Scenarios
        expect(customLoggerService.logger.info).toHaveBeenCalledWith(logMessage, { reqId: testTracerId });
    });

    test('Successful - Scenario 2', async () => {
        //Bypass return
        jest.spyOn(rTracer, 'id').mockImplementation(() => undefined);

        let logMessage = 'testLogMessage'
        let logResult: any = {};
        logResult = await customLoggerService.log(logMessage);
        //console.log('log Success - Scenario 2', logResult)

        //Assert Test Scenarios
        expect(customLoggerService.logger.info).toHaveBeenCalledWith(logMessage);
    });
  });

  describe('error', () => {
    test('Successful - Scenario 1', async () => {
        let logMessage = 'testLogMessage'
        let logResult: any = {};
        logResult = await customLoggerService.error(logMessage);
        //console.log('error Success - Scenario1', logResult)

        //Assert Test Scenarios
        expect(customLoggerService.logger.error).toHaveBeenCalledWith(logMessage, { reqId: testTracerId });
    });

    test('Successful - Scenario 2', async () => {
        //Bypass return
        jest.spyOn(rTracer, 'id').mockImplementation(() => undefined);

        let logMessage = 'testLogMessage'
        let logResult: any = {};
        logResult = await customLoggerService.error(logMessage);
        //console.log('error Success - Scenario 2', logResult)

        //Assert Test Scenarios
        expect(customLoggerService.logger.error).toHaveBeenCalledWith(logMessage);
    });
  });

  describe('warn', () => {
    test('Successful - Scenario 1', async () => {
        let logMessage = 'testLogMessage'
        let logResult: any = {};
        logResult = await customLoggerService.warn(logMessage);
        //console.log('warn Success - Scenario1', logResult)

        //Assert Test Scenarios
        expect(customLoggerService.logger.log).toHaveBeenCalledWith(logMessage, { reqId: testTracerId });
    });

    test('Successful - Scenario 2', async () => {
        //Bypass return
        jest.spyOn(rTracer, 'id').mockImplementation(() => undefined);

        let logMessage = 'testLogMessage'
        let logResult: any = {};
        logResult = await customLoggerService.warn(logMessage);
        //console.log('warn Success - Scenario 2', logResult)

        //Assert Test Scenarios
        expect(customLoggerService.logger.log).toHaveBeenCalledWith(logMessage);
    });
  });

  describe('debug', () => {
    test('Successful - Scenario 1', async () => {
        let logMessage = 'testLogMessage'
        let logResult: any = {};
        logResult = await customLoggerService.debug(logMessage);
        //console.log('debug Success - Scenario1', logResult)

        //Assert Test Scenarios
        expect(customLoggerService.logger.debug).toHaveBeenCalledWith(logMessage, { reqId: testTracerId });
    });

    test('Successful - Scenario 2', async () => {
        //Bypass return
        jest.spyOn(rTracer, 'id').mockImplementation(() => undefined);

        let logMessage = 'testLogMessage'
        let logResult: any = {};
        logResult = await customLoggerService.debug(logMessage);
        //console.log('debug Success - Scenario 2', logResult)

        //Assert Test Scenarios
        expect(customLoggerService.logger.debug).toHaveBeenCalledWith(logMessage);
    });
  });
});


