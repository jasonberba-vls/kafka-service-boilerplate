
import { Test, TestingModule } from '@nestjs/testing';
import { InternalApiWrapper } from '../../common/wrapper/internalApiWrapper';
import { ConfigService } from "@nestjs/config";
import rTracer from 'cls-rtracer';
import initializeAxios from './axiosInstance';

describe('InternalApiWrapper', () => {
  //Initialize Objects and Mocks
  let internalApiWrapper: InternalApiWrapper;
  let testUrl = 'www.testUrl.test';
  let testBody = { body: 'testBody' };
  let testTracerId = '123';
  
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
        InternalApiWrapper,
        ConfigService
    ],
    }).compile();

    //Initialize Object to Test
    internalApiWrapper = module.get<InternalApiWrapper>(InternalApiWrapper);
    // customLoggerService.logger = module.get<Logger>(Logger);

    //Map response to Mock Objects
    jest.spyOn(rTracer, 'id').mockImplementation(() => testTracerId);
    internalApiWrapper.axiosInstance.get = jest.fn();
    internalApiWrapper.axiosInstance.post = jest.fn();
    internalApiWrapper.axiosInstance.put = jest.fn();
  });

  afterEach(() => {
    process.env = originalEnv;
  });
  
  describe('Initialization', () => {
    test('Success', () => {
        //Assert Test Scenarios
        expect(internalApiWrapper).toBeDefined();
    });
  });

  describe('get', () => {
    test('Successful', async () => {
        let internalApiWrapperResult: any = {};
        internalApiWrapperResult = internalApiWrapper.get(testUrl);
        //console.log('get Success', internalApiWrapperResult)

        //Assert Test Scenarios
        expect(internalApiWrapper.axiosInstance.get).toHaveBeenCalledWith(testUrl, { params: undefined });
    });
  });

  describe('post', () => {
    test('Successful', async () => {
        let internalApiWrapperResult: any = {};
        internalApiWrapperResult = internalApiWrapper.post(testUrl, testBody);
        //console.log('post Success', internalApiWrapperResult)

        //Assert Test Scenarios
        expect(internalApiWrapper.axiosInstance.post).toHaveBeenCalledWith(testUrl, testBody, { params: undefined });
    });
  });

  describe('put', () => {
    test('Successful', async () => {
        let internalApiWrapperResult: any = {};
        internalApiWrapperResult = internalApiWrapper.put(testUrl, testBody);
        //console.log('put Success', internalApiWrapperResult)

        //Assert Test Scenarios
        expect(internalApiWrapper.axiosInstance.put).toHaveBeenCalledWith(testUrl, testBody, { params: undefined });
    });
  });
});


