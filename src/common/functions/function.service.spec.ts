import { Test, TestingModule } from '@nestjs/testing';
import { FunctionService } from './function.service';
import { isAlphanumeric } from 'class-validator';
import dayjs from 'dayjs';

describe('FunctionService', () => {
  //Initialize Objects and Mocks
  let functionService: FunctionService;
  const merchantUrl : string = 'https://merchantUrl';

  //Initialize  Env Object
  const originalEnv = process.env;

  beforeEach(async () => {
    jest.resetModules();
      process.env = {
        ENC_KEY: 's233EwqnaEDWQ2dhas421y6dhaiE6329',
      };

    const module: TestingModule = await Test.createTestingModule({
    providers: [
        FunctionService,
    ],
    })
    .compile();

    //Intiialize Object to Test
    functionService = module.get<FunctionService>(FunctionService);
    functionService.merchantURL = merchantUrl;
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('Initialization', () => {
    test('Success', () => {
        
        //Assert Test Scenarios
        expect(functionService).toBeDefined();
    });
  });

  describe('getBankCodeIFSC', () => {
    test('Successful', () => {
        let valueToTest = 'ABCD_1234';

        let getBankCodeIFSCResult: any; 
        getBankCodeIFSCResult = functionService.getBankCodeIFSC(valueToTest);
        //console.log('getBankCodeIFSC Success', getBankCodeIFSCResult);

        //Assert Test Scenarios
        expect(getBankCodeIFSCResult).not.toBeUndefined();
        expect(getBankCodeIFSCResult).toBe(valueToTest.substring(0,4));
    });
  });

  describe('getCallbackURL', () => {
    test('Successful', () => {
        let valueToTest = 'ABCD_1234';

        let getCallbackURLResult: any; 
        getCallbackURLResult = functionService.getCallbackURL(valueToTest);
        //console.log('getCallbackURL Success', getCallbackURLResult);

        //Assert Test Scenarios
        expect(getCallbackURLResult).not.toBeUndefined();
        expect(getCallbackURLResult).toBe(merchantUrl + '/api/callback/withdrawal/' + valueToTest);
    });
  });

  describe('getCallbackRedirectURL', () => {
    test('Successful', () => {
        let valueToTest = 'ABCD_1234';

        let getCallbackRedirectURLResult: any; 
        getCallbackRedirectURLResult = functionService.getCallbackRedirectURL(valueToTest);
        //console.log('getCallbackRedirectURL Success', getCallbackRedirectURLResult);

        //Assert Test Scenarios
        expect(getCallbackRedirectURLResult).not.toBeUndefined();
        expect(getCallbackRedirectURLResult).toBe(merchantUrl + '/api/callback/deposit/redirect/' + valueToTest);
    });
  });

  describe('getTransferURL', () => {
    test('Successful', () => {
        let getTransferURLResult: any; 
        getTransferURLResult = functionService.getTransferURL();
        //console.log('getTransferURL Success', getTransferURLResult);

        //Assert Test Scenarios
        expect(getTransferURLResult != undefined).not.toBeUndefined();
        expect(getTransferURLResult).toBe(merchantUrl + '/landingpage/transfer');
    });
  });

  describe('getSuccessURL', () => {
    test('Successful', () => {
        let getSuccessURLResult: any; 
        getSuccessURLResult = functionService.getSuccessURL();
        //console.log('getSuccessURL Success', getSuccessURLResult);

        //Assert Test Scenarios
        expect(getSuccessURLResult).not.toBeUndefined();
        expect(getSuccessURLResult).toBe(merchantUrl + '/landingpage/success');
    });
  });

  describe('getFailedURL', () => {
    test('Successful', () => {
        let getFailedURLResult: any; 
        getFailedURLResult = functionService.getFailedURL();
        //console.log('getFailedURL Success', getFailedURLResult);

        //Assert Test Scenarios
        expect(getFailedURLResult).not.toBeUndefined();
        expect(getFailedURLResult).toBe(merchantUrl + '/landingpage/failed');
    });
  });

  describe('getTimeStamp', () => {
    test('Successful', () => {
        let getTimeStampResult: any; 
        getTimeStampResult = functionService.getTimeStamp();
        // console.log('getTimeStamp Success', getTimeStampResult);

        //Assert Test Scenarios
        expect(getTimeStampResult).not.toBeUndefined();
        expect(Number(getTimeStampResult)).toBeGreaterThan(0);
    });
  });

  describe('getRandomId', () => {
    test('Successful', () => {
        let getRandomIdParam = 10;

        let getRandomIdResult: any; 
        getRandomIdResult = functionService.getRandomId(getRandomIdParam);
        //console.log('getRandomId Success', getRandomIdResult);
        
        //Assert Test Scenarios
        expect(getRandomIdResult).not.toBeUndefined();
        expect(getRandomIdResult.length).toBe(getRandomIdParam);
        expect(isAlphanumeric(getRandomIdResult)).toBe(true);
    });
  });

  describe('getDateToday', () => {
    test('Successful', () => {
        let getDateTodayResult: any; 
        getDateTodayResult = functionService.getDateToday();
        //console.log('getDateToday Success', getDateTodayResult);
        
        //Assert Test Scenarios
        expect(getDateTodayResult).not.toBeUndefined();
        expect(Date.parse(getDateTodayResult)).toBeGreaterThan(0);
    });
  });

  describe('getDateIST', () => {
    test('Successful', () => {
        let valueToTest : any[] = ['2024-06-21 15:18:26', 'YYYY-MM-DD'];

        let getDateISTResult: any; 
        getDateISTResult = functionService.getDateIST(...valueToTest);
        //console.log('getDateIST Success', getDateISTResult);
        
        //Assert Test Scenarios
        expect(getDateISTResult).not.toBeUndefined();
        expect(Date.parse(getDateISTResult)).toBeGreaterThan(0);
    });
  });

  describe('getDateNowIST', () => {
    test('Successful', () => {
        let valueToTest : any[] = ['YYYY-MM-DD'];

        let getDateNowISTResult: any; 
        getDateNowISTResult = functionService.getDateNowIST(...valueToTest);
        //console.log('getDateNowIST Success', getDateNowISTResult);
        
        //Assert Test Scenarios
        expect(getDateNowISTResult).not.toBeUndefined();
        expect(Date.parse(getDateNowISTResult)).toBeGreaterThan(0);
    });
  });

  describe('getTurboPayQueryPayload', () => {
    test('Successful', () => {
        let turboPayTestOrderId = 'testTurboPayOrderId';
        let valueToTest: any[] = [turboPayTestOrderId];

        let getTurboPayQueryPayloadResult: any; 
        getTurboPayQueryPayloadResult = functionService.getTurboPayQueryPayload(...valueToTest);
        //console.log('getTurboPayQueryPayload Success', getTurboPayQueryPayloadResult);

        const decodedString = Buffer.from(getTurboPayQueryPayloadResult, 'base64').toString('utf-8');
        //console.log('getTurboPayQueryPayload decodedString', decodedString);

        //Assert Test Scenarios
        expect(getTurboPayQueryPayloadResult).not.toBeUndefined();
        expect(decodedString != undefined).not.toBeUndefined();
        expect(JSON.parse(decodedString).body.orderId).toBe(turboPayTestOrderId);
    });
  });

  describe('decodeToBase64', () => {
    test('Successful', () => {
        let turboPayTestOrderId = 'testTurboPayOrderId';
        let base64TestValue = Buffer.from(turboPayTestOrderId).toString('base64');
        let valueToTest: any[] = [base64TestValue];

        let decodeToBase64Result: any; 
        decodeToBase64Result = functionService.decodeToBase64(...valueToTest);
        //console.log('decodeToBase64 Success', decodeToBase64Result);

        //Assert Test Scenarios
        expect(decodeToBase64Result).not.toBeUndefined();
        expect(decodeToBase64Result == turboPayTestOrderId).toBe(true);
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
        compareJSONValuesResult = functionService.compareJSONValues(...valueToTest);
        //console.log('compareJSONValues Success', compareJSONValuesResult);

        //Assert Test Scenarios
        expect(compareJSONValuesResult).not.toBeUndefined();
        expect(compareJSONValuesResult).toBe(true);
    });
  });

  describe('convertToJason', () => {
    test('Successful', () => {
        let jsonStringValue : string = '{ "test1": "test1234", "test2": "test12345" }';
        let valueToTest: any[] = [jsonStringValue];

        let convertToJasonResult: any; 
        convertToJasonResult = functionService.convertToJason(valueToTest);
        //console.log('convertToJason Success', convertToJasonResult);

        //Assert Test Scenarios
        expect(convertToJasonResult).not.toBeUndefined();
        let jsonStringResult = JSON.stringify(convertToJasonResult);
        expect(jsonStringResult).not.toBeUndefined();
        expect(jsonStringResult).not.toBe('');
        expect(JSON.parse(jsonStringResult)).not.toBeUndefined();;
    });

  });

  describe('getDateTodayV2', () => {
    test('Successful', () => {
        let valueToTest : any[] = ['2024-06-01 15:18:26'];

        let getDateTodayV2Result: any; 
        getDateTodayV2Result = functionService.getDateTodayV2(...valueToTest);
        //console.log('getDateTodayV2 Success', getDateTodayV2Result);
        
        //Assert Test Scenarios
        expect(getDateTodayV2Result).not.toBeUndefined();
        expect(dayjs(valueToTest[0]).format('YYYY-MM-DD')).toBe(getDateTodayV2Result);
    });
  });

  describe('concatinateValues', () => {
    test('Successful', () => {
        let valueToTest : any[] = ['AB', 'CD', '12', '34'];

        let concatinateValuesResult: any; 
        concatinateValuesResult = functionService.concatinateValues(...valueToTest);
        //console.log('concatinateValues Success', concatinateValuesResult);
        
        //Assert Test Scenarios
        expect(concatinateValuesResult).not.toBeUndefined();
        expect(concatinateValuesResult).toBe('ABCD1234');
    });
  });

  describe('encodeToBase64', () => {
    test('Successful', () => {
        let base64StringtoEncode = 'base64StringtoEncodeValue'
        let valueToTest : any[] = [base64StringtoEncode];

        let encodeToBase64Result: any; 
        encodeToBase64Result = functionService.encodeToBase64(...valueToTest);
        //console.log('encodeToBase64 Success', encodeToBase64Result);
        
        //Assert Test Scenarios
        expect(encodeToBase64Result).not.toBeUndefined();
        expect(Buffer.from(encodeToBase64Result, 'base64').toString('utf-8')).toBe(base64StringtoEncode);
    });
  });

});