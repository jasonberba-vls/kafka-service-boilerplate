import { Test, TestingModule } from '@nestjs/testing';
import { PostStatusInterceptor } from '../../common/interceptors/post-status.interceptor';
import { CallHandler, HttpStatus } from '@nestjs/common';
import { map } from 'rxjs/operators';
import { Observable, of } from 'rxjs';

describe('PostStatusInterceptor', () => {
  //Initialize Objects and Mocks
  let postStatusInterceptor: PostStatusInterceptor;
  let mockcallHandler: jest.Mocked<CallHandler>;


 const executionContext: any = {
    switchToHttp: () => ({
      getRequest: () => ({
        method: 'POST'
      }),
      getResponse: () => ({
        statusCode: HttpStatus.CREATED,
        status: () => {}
    }),
    }),
  }
  
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
        PostStatusInterceptor
    ],
    }).compile();

    //Initialize Object to Test
    postStatusInterceptor = module.get<PostStatusInterceptor>(PostStatusInterceptor);

    //jest.spyOn(mockcallHandler, 'handle').mockImplementation(() => ({source: undefined,subscribe: undefined,operator: undefined,lift: undefined, forEach: undefined, toPromise: undefined, pipe: undefined}));
  });

  afterEach(() => {
    process.env = originalEnv;
  })
  
  describe('Initialization', () => {
    test('Success', () => {
        //Assert Test Scenarios
        expect(postStatusInterceptor).toBeDefined();
    });
  });

  describe('intercept', () => {
    test('Success - Scenario 1', async () => {
        //Assign CallHandler Value
        let returnValue = 'testValue';
        mockcallHandler = { handle: jest.fn(() => of(returnValue)) };

        let postStatusInterceptorResult: any = {};
        postStatusInterceptorResult = await postStatusInterceptor.intercept(executionContext, mockcallHandler);
        //console.log('PostStatusInterceptor Success - Scenario 1', postStatusInterceptorResult)

        //Assert Test Scenarios
        expect(postStatusInterceptorResult).not.toBeUndefined();
        postStatusInterceptorResult.subscribe({
          next: value => {
            //console.log('value', value);
            expect(value).toBe(returnValue);
          },
        });
    });

    test('Success - Scenario 2 ', async () => {
      //Assign CallHandler Value
      let returnValue = 'testValue';
      const callHandler: any = {
          handle: () => ({
              pipe: (test: any) => (returnValue)
          }),
        }

      let postStatusInterceptorResult: any = {};
      postStatusInterceptorResult = await postStatusInterceptor.intercept(executionContext, callHandler);
      //console.log('PostStatusInterceptor Success - Scenario 2', postStatusInterceptorResult)

      //Assert Test Scenarios
      expect(postStatusInterceptorResult).not.toBeUndefined();
      expect(postStatusInterceptorResult).toBe(returnValue);
  });
  });
});

