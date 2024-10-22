import { Test, TestingModule } from '@nestjs/testing';
import { EncryptionService } from './encryption.service';

describe('EncryptionService', () => {
  //Initialize Objects and Mocks
  let encryptionService: EncryptionService;

  //Initialize  Env Object
  const originalEnv = process.env;

  beforeEach(async () => {
    jest.resetModules();
      process.env = {
        ENC_KEY: 's233EwqnaEDWQ2dhas421y6dhaiE6329',
      };

    const module: TestingModule = await Test.createTestingModule({
    providers: [
        EncryptionService,
    ],
    })
    .compile();

    //Intiialize Object to Test
    encryptionService = module.get<EncryptionService>(EncryptionService);
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('Initialization', () => {
    test('Success', () => {
        
        //Assert Test Scenarios
        expect(encryptionService).toBeDefined();
    });
  });

  describe('encryptData', () => {
    test('Successful', () => {
      let valueToEncrypt = 'ABC_123';

      let encryptDataResult: any; 
      encryptDataResult = encryptionService.encryptData(valueToEncrypt);
      //console.log('encryptDataResult', encryptDataResult);

      //Assert Test Scenarios
      expect(encryptDataResult).not.toBeUndefined();
      expect(encryptDataResult).not.toBe('');
    });
  });

  describe('decryptData', () => {
    test('Successful', () => {
      let valueToDecrypt = 'de9de2314ee9e58c688af5feb93bcf49'; 
      let expectedValueDecrypted = 'ABC_123';

      let decryptDataResult: any; 
      decryptDataResult = encryptionService.decryptData(valueToDecrypt);
      //console.log('decryptDataResult', decryptDataResult);

      //Assert Test Scenarios
      expect(decryptDataResult).not.toBeUndefined();
      expect(decryptDataResult).not.toBe('');
      expect(decryptDataResult).toBe(expectedValueDecrypted);
    });

    test('Failed - Empty Input', () => {
        let valueToDecrypt = '';

        let decryptDataResult: any; 
        decryptDataResult = encryptionService.decryptData(valueToDecrypt);
        //console.log('decryptDataResult', decryptDataResult);

        //Assert Test Scenarios
        expect(decryptDataResult).not.toBeUndefined();
        expect(decryptDataResult).toBe('');
    });

    test('Failed - Invalid Input', () => {
      let valueToDecrypt = '123456';

      let decryptDataResult: any; 
      decryptDataResult = encryptionService.decryptData(valueToDecrypt);
      //console.log('decryptDataResult', decryptDataResult);

      //Assert Test Scenarios
      expect(decryptDataResult).not.toBeUndefined();
      expect(decryptDataResult).toBe('');
  });
  });
});