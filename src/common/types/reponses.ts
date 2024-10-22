export enum ResponseMessages {
    UNAUTHORIZED = 'UNAUTHORIZED',
    SuccessfullyCalled = 'Successfully Called',
    RequestSuccessful = 'Request Successful',
    InvalidParameter = 'Invalid parameter',
    InvalidSignature = 'Invalid signature',
    ErrorProcessingRequest = 'Error in processing request',
    FailedTransaction = 'Failed Transaction',
    Pending = 'Pending',
    TransactionDoesNotExist = 'Transaction Does Not Exist',
    DepositRequestAccepted = 'Deposit request accepted',
    WithdrawalRequestAccepted = 'Withdrawal request accepted',
    Successful = 'Successful',
  }

export enum GetBalanceStatusCodes {
    Successful = '00',
    InvalidParameter = '30',
    InvalidSignature = '31',
    ErrorProcessingRequest = '32',
  }

export enum QueryStatusCodes {
    Successful = '00',
    FailedTransaction = '10',
    Pending = '20',
    InvalidParameter = '30',
    InvalidSignature = '31',
    ErrorProcessingRequest = '32',
    TransactionDoesNotExist = '33',
  }

export enum DepositStatusCodes {
    Successful = '00',
    InvalidParameter = '10',
    InvalidSignature = '11',
    ErrorProcessingRequest = '12',
  }

export enum WithdrawalStatusCodes {
    Successful = '00',
    InvalidParameter = '10',
    InvalidSignature = '11',
    ErrorProcessingRequest = '12',
  }

export enum GetCryptoSupportedCoinsStatusCodes {
    Successful = '00',
    ErrorProcessingRequest = '10',
  }

export enum ResponseLoggingType {
    Message = 'Message',
    Error = 'Error',
  }

export const HandledErrorForLogging = [
    'Invalid version',
    'Invalid parameter',
    'Unable to find merchant',
    'Unable to find merchant currency',
    'Unable to find country',
    'Invalid payment type',
    'Invalid limit setting',
    'Invalid version',
    'No gateway available',
    'Invalid exchange rate setting',
    'Merchant is not active',
    'Unable to find merchant currency wallet',
    'Merchant currency wallet is not active',
    'Merchant wallet balance is not enough for transaction',
    'Unable to find gateway merchant',
    'Gateway merchant is not active',
    'Unable to find gateway',
    'Gateway is not active',
    'Unable to find vendor merchant wallet',
    'Merchant currency is not active',
    'Unable to create withdrawal transaction',
];

export const HandledErrorForResponse = [
    'System under maintenance',
    'Request Amount not within limit',
    'Daily limit has been reached',
    'Merchant Reference Id already exists'
];

export class ProcessedErrorReturn {
    responseLoggingType?: ResponseLoggingType;
    message?: string;

    constructor(){
        this.responseLoggingType = ResponseLoggingType.Error;
        this.message = "";
    }
}