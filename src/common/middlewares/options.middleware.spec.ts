import { Test, TestingModule } from '@nestjs/testing';
import * as optionsMiddlewareMethods from '../../common/middlewares/options.middleware';
import { CallHandler, HttpStatus } from '@nestjs/common';
import { map } from 'rxjs/operators';
import { Observable, of } from 'rxjs';
import { Request, Response } from 'express';

describe('OptionsMiddleware', () => {
  //Initialize Objects and Mocks
  let requestObject = {
    method: 'OPTIONS'
  } as Request;

  let responseObject = {} as unknown as Response;
  responseObject.status = jest.fn(() => responseObject); // chained
  responseObject.set = jest.fn();
  responseObject.send = jest.fn();
  responseObject.end = jest.fn();

  let nextObject = jest.fn();
  
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
    ],
    }).compile();

    //Initialize Object to Test
  });

  afterEach(() => {
    process.env = originalEnv;
  })
  
  describe('Initialization', () => {
    test('Success', () => {
        //Assert Test Scenarios
        expect(optionsMiddlewareMethods).toBeDefined();
    });
  });

  describe('optionsMiddleware', () => {
    test('Success - Scenario 1', async () => {
        let optionsMiddlewareResult: any = {};
        optionsMiddlewareResult = optionsMiddlewareMethods.optionsMiddleware(requestObject, responseObject, nextObject);
        //console.log('optionsMiddleware Success - Scenario 1', optionsMiddlewareResult)

        //Assert Test Scenarios
        expect(responseObject.send).toHaveBeenCalled();
    });

    test('Success - Scenario 2', async () => {
        requestObject.method = 'POST';

        let optionsMiddlewareResult: any = {};
        optionsMiddlewareResult = optionsMiddlewareMethods.optionsMiddleware(requestObject, responseObject, nextObject);
        //console.log('optionsMiddleware Success - Scenario 2', optionsMiddlewareResult)

        //Assert Test Scenarios
        expect(nextObject).toHaveBeenCalled();
    });
  });
});

