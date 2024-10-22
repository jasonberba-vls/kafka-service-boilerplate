import { Test, TestingModule } from '@nestjs/testing';
import { HealthController } from './health.controller';

describe('HealthController', () => {
  //Initialize Objects and Mocks
  let healthController: HealthController;
  
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
        HealthController,
    ],
    }).compile();

    //Initialize Object to Test
    healthController = module.get<HealthController>(HealthController);
  });

  afterEach(() => {
    process.env = originalEnv;
  });
  
  describe('Initialization', () => {
    test('Success', () => {
        //Assert Test Scenarios
        expect(healthController).toBeDefined();
    });
  });

  describe('/health', () => {
    test('Successful', async () => {
        let checkResult: any = {};
        
        checkResult = await healthController.check();
        //console.log('Check Result Successful', checkResult);

        //Assert Test Scenarios
        expect(checkResult).not.toBeUndefined();
        expect(checkResult).toBe('ok');
    });

  });
});