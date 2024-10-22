import { Test, TestingModule } from '@nestjs/testing';
import { ValidatorFunctionService } from './validatorFunction.service';
import { isAlphanumeric } from 'class-validator';
import dayjs from 'dayjs';

describe('ValidatorFunctionService', () => {
  //Initialize Objects and Mocks
  let validatorFunctionService: ValidatorFunctionService;

  //Initialize  Env Object
  const originalEnv = process.env;

  beforeEach(async () => {
    jest.resetModules();
      process.env = {
        ENC_KEY: 's233EwqnaEDWQ2dhas421y6dhaiE6329',
      };

    const module: TestingModule = await Test.createTestingModule({
    providers: [
        ValidatorFunctionService,
    ],
    })
    .compile();

    //Intiialize Object to Test
    validatorFunctionService = module.get<ValidatorFunctionService>(ValidatorFunctionService);
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('Initialization', () => {
    test('Success', () => {
        
        //Assert Test Scenarios
        expect(validatorFunctionService).toBeDefined();
    });
  });

  describe('compareJSONValues', () => {
    test('Successful', () => {
        let jsonValue1 : any = {
            test3: 'test456',
            test1: 'test1234',
            test2: 'test12345'
        };
        let jsonValue2 : any = {
            test3: 'test456',
            test1: 'test1234',
            test2: 'test12345'
        };
        let valueToTest: any[] = [jsonValue1, jsonValue2];

        let compareJSONValuesResult: any; 
        compareJSONValuesResult = validatorFunctionService.compareJSONValues(...valueToTest);
        //console.log('compareJSONValues Success', compareJSONValuesResult);

        //Assert Test Scenarios
        expect(compareJSONValuesResult).not.toBeUndefined();
        expect(compareJSONValuesResult).toBe(true);
    });
  });

  describe('compareMultipleStringValues', () => {
    test('Successful', () => {
      let valueToTest: any[] = ['test123', 'test123', 'test123'];

      let compareMultipleStringValuesResult: any; 
      compareMultipleStringValuesResult = validatorFunctionService.compareMultipleStringValues(...valueToTest);
      //console.log('compareMultipleStringValues Success', compareMultipleStringValuesResult);

      //Assert Test Scenarios
      expect(compareMultipleStringValuesResult).not.toBeUndefined();
      expect(compareMultipleStringValuesResult).toBe(true);
    });
  });

  describe('compareMultipleNumberValues', () => {
    test('Successful', () => {
      let valueToTest: any[] = [123, 123, 123];

      let compareMultipleNumberValuesResult: any; 
      compareMultipleNumberValuesResult = validatorFunctionService.compareMultipleNumberValues(...valueToTest);
      //console.log('compareMultipleNumberValues Success', compareMultipleNumberValuesResult);

      //Assert Test Scenarios
      expect(compareMultipleNumberValuesResult).not.toBeUndefined();
      expect(compareMultipleNumberValuesResult).toBe(true);
    });

    test('Failed - Invalid Input', () => {
      let valueToTest: any[] = undefined;

      let compareMultipleNumberValuesResult: any; 
      compareMultipleNumberValuesResult = validatorFunctionService.compareMultipleNumberValues(valueToTest);
      //console.log('compareMultipleNumberValues Invalid Input', compareMultipleNumberValuesResult);

      //Assert Test Scenarios
      expect(compareMultipleNumberValuesResult).not.toBeUndefined();
      expect(compareMultipleNumberValuesResult).toBe(false);
    });
  });

  describe('compareMultipleFloatValues', () => {
    test('Successful', () => {
      let valueToTest: any[] = [123.45, 123.45, 123.45];

      let compareMultipleFloatValuesResult: any; 
      compareMultipleFloatValuesResult = validatorFunctionService.compareMultipleFloatValues(...valueToTest);
      //console.log('compareMultipleFloatValues Success', compareMultipleFloatValuesResult);

      //Assert Test Scenarios
      expect(compareMultipleFloatValuesResult).not.toBeUndefined();
      expect(compareMultipleFloatValuesResult).toBe(true);
    });
    test('Failed - Invalid Input', () => {
      let valueToTest: any[] = undefined;

      let compareMultipleFloatValuesResult: any; 
      compareMultipleFloatValuesResult = validatorFunctionService.compareMultipleFloatValues(valueToTest);
      //console.log('compareMultipleFloatValues Invalid Input', compareMultipleFloatValuesResult);

      //Assert Test Scenarios
      expect(compareMultipleFloatValuesResult).not.toBeUndefined();
      expect(compareMultipleFloatValuesResult).toBe(false);
    });
  });

  describe('amountIsLessThan1', () => {
    test('Successful', () => {
      let valueToTest: any[] = [1.45, 1];

      let amountIsLessThan1Result: any; 
      amountIsLessThan1Result = validatorFunctionService.amountIsLessThan1(...valueToTest);
      //console.log('amountIsLessThan1 Success', amountIsLessThan1Result);

      //Assert Test Scenarios
      expect(amountIsLessThan1Result).not.toBeUndefined();
      expect(amountIsLessThan1Result).toBe(true);
    });

    test('Failed - Invalid Input', () => {
      let valueToTest: any[] = [null, null];

      let amountIsLessThan1Result: any; 
      amountIsLessThan1Result = validatorFunctionService.amountIsLessThan1(...valueToTest);
      //console.log('amountIsLessThan1 Invalid Input', amountIsLessThan1Result);

      //Assert Test Scenarios
      expect(amountIsLessThan1Result).not.toBeUndefined();
      expect(amountIsLessThan1Result).toBe(false);
    });
  });
});