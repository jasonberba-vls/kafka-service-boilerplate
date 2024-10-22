import { Test, TestingModule } from '@nestjs/testing';
import * as helperFunctionMethods from './helperFunction';
import { Request } from 'express';

describe('HelperFunction', () => {
  //Initialize  Env Object
  const originalEnv = process.env;

  beforeEach(async () => {
    jest.resetModules();
      process.env = {
        ENC_KEY: 's233EwqnaEDWQ2dhas421y6dhaiE6329',
      };

    const module: TestingModule = await Test.createTestingModule({
    providers: [],
    })
    .compile();
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('Initialization', () => {
    test('Success', () => {
        //Assert Test Scenarios
        expect(helperFunctionMethods).toBeDefined();
    });
  });

  describe('getWordsBetweenCurlies', () => {
    test('Successful', () => {
        let valueToCheck = 'testValue'
        let valueSeparator = '|';
        let valueToTest = valueSeparator + valueToCheck + valueSeparator;

        let getWordsBetweenCurliesResult: any; 
        getWordsBetweenCurliesResult = helperFunctionMethods.getWordsBetweenCurlies(valueToTest, valueSeparator, valueSeparator);
        //console.log('getWordsBetweenCurliesResult Success', getWordsBetweenCurliesResult);

        //Assert Test Scenarios
        expect(getWordsBetweenCurliesResult).not.toBeUndefined();
        expect(getWordsBetweenCurliesResult.length).toBe(1);
        expect(getWordsBetweenCurliesResult[0]).toBe(valueToCheck);

        //Additional object for result Array
        let valueToCheck2 = 'testValue2'
        valueToTest = valueToTest + valueSeparator + valueToCheck2 + valueSeparator;

        getWordsBetweenCurliesResult = helperFunctionMethods.getWordsBetweenCurlies(valueToTest, valueSeparator, valueSeparator);
        //console.log('getWordsBetweenCurliesResult Additional object Success', getWordsBetweenCurliesResult);

        //Assert Test Scenarios
        expect(getWordsBetweenCurliesResult).not.toBeUndefined();
        expect(getWordsBetweenCurliesResult.length).toBe(2);
        expect(getWordsBetweenCurliesResult[0]).toBe(valueToCheck);
        expect(getWordsBetweenCurliesResult[1]).toBe(valueToCheck2);
    });
  });

  describe('getNestedDataValue', () => {
    test('Successful', () => {
        let valueToCheck = 'testValue'
        let jsonDataObject: any = {
            firstNest: {
                secondNest: {
                    finalObject : valueToCheck
                }
            }
        };
        let keyPath = 'firstNest/secondNest/finalObject';

        let getNestedDataValueResult: any; 
        getNestedDataValueResult = helperFunctionMethods.getNestedDataValue(jsonDataObject, keyPath.split("/"));
        //console.log('getNestedDataValue Success', getNestedDataValueResult);

        //Assert Test Scenarios
        expect(getNestedDataValueResult).not.toBeUndefined();
        expect(getNestedDataValueResult).toBe(valueToCheck);
    });

    test('Failed - Invalid Key', () => {
        let valueToCheck = 'testValue'
        let jsonDataObject: any = {
            firstNest: {
                secondNest: {
                    finalObject : valueToCheck
                }
            }
        };
        let keyPath = 'firstNest/secondNest/invalidPath';

        let getNestedDataValueResult: any; 
        getNestedDataValueResult = helperFunctionMethods.getNestedDataValue(jsonDataObject, keyPath.split("/"));
        //console.log('getNestedDataValue Empty Return', getNestedDataValueResult);

        //Assert Test Scenarios
        expect(getNestedDataValueResult).not.toBeUndefined();
        expect(getNestedDataValueResult).not.toBe(valueToCheck);
    });

    test('Failed - Invalid Path', () => {
        let valueToCheck = 'testValue'
        let jsonDataObject: any = {
            firstNest: {
                secondNest: {
                    finalObject : valueToCheck
                }
            }
        };
        let keyPath = 'invalidPath/secondNest/finalObject';

        let getNestedDataValueResult: any; 
        getNestedDataValueResult = helperFunctionMethods.getNestedDataValue(jsonDataObject, keyPath.split("/"));
        //console.log('getNestedDataValue Empty Return 2', getNestedDataValueResult);

        //Assert Test Scenarios
        expect(getNestedDataValueResult).not.toBeUndefined();
        expect(getNestedDataValueResult).not.toBe(valueToCheck);
    });
  });

  describe('getClientIp', () => {
    test('Successful via cf-connecting-ip', () => {
        let valueToCheck = '6.9.6.9'
        let valueToTest = {
            body: {
                "test": "test",
            },
        } as Request;
        valueToTest.headers = {};
        valueToTest.headers["cf-connecting-ip"] = valueToCheck;

        let getClientIpResult: any; 
        getClientIpResult = helperFunctionMethods.getClientIp(valueToTest);
        //console.log('getClientIp Success', getClientIpResult);

        //Assert Test Scenarios
        expect(getClientIpResult).not.toBeUndefined();
        expect(getClientIpResult).toBe(valueToCheck);
    });

    test('Successful via x-forwarded-for', () => {
        let valueToCheck = '6.9.6.9'
        let valueToTest = {
            body: {
                "test": "test",
            },
        } as Request;
        valueToTest.headers = {};
        valueToTest.headers["x-forwarded-for"] = valueToCheck;

        let getClientIpResult: any; 
        getClientIpResult = helperFunctionMethods.getClientIp(valueToTest);
        //console.log('getClientIp Success 2', getClientIpResult);

        //Assert Test Scenarios
        expect(getClientIpResult).not.toBeUndefined();
        expect(getClientIpResult).toBe(valueToCheck);
    });

    test('Failed - Missing Header Key', () => {
        let valueToCheck = '6.9.6.9'
        let valueToTest = {
            body: {
                "test": "test",
            },
        } as Request;
        valueToTest.headers = {};

        let getClientIpResult: any; 
        getClientIpResult = helperFunctionMethods.getClientIp(valueToTest);
        //console.log('getClientIp Empty Return', getClientIpResult);

        //Assert Test Scenarios
        expect(getClientIpResult).not.toBeUndefined();
        expect(getClientIpResult).toBe('');
    });
  });

  describe('getHostHeader', () => {
    test('Successful', () => {
        let valueToCheck = 'hostValue'
        let valueToTest = {
            body: {
                "test": "test",
            },
        } as Request;
        valueToTest.headers = {};
        valueToTest.headers["host"] = valueToCheck;

        let getHostHeaderResult: any; 
        getHostHeaderResult = helperFunctionMethods.getHostHeader(valueToTest);
        //console.log('getHostHeader Success', getHostHeaderResult);

        //Assert Test Scenarios
        expect(getHostHeaderResult).not.toBeUndefined();
        expect(getHostHeaderResult).toBe(valueToCheck);
    });

    test('Successful - Empty Host', () => {
        let valueToTest = {
            body: {
                "test": "test",
            },
        } as Request;
        valueToTest.headers = {};

        let getHostHeaderResult: any; 
        getHostHeaderResult = helperFunctionMethods.getHostHeader(valueToTest);
        //console.log('getHostHeader Success - Empty Host'', getHostHeaderResult);

        //Assert Test Scenarios
        expect(getHostHeaderResult).not.toBeUndefined();
        expect(getHostHeaderResult).toBe('');
    });
  });
});