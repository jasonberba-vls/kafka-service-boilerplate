
export class ValidatorFunctionService {
  constructor() {
  }

  compareJSONValues(...params): boolean {
    const arrayParams0: { key: string; value: any }[] = [];
          Object.entries(params[0]).forEach(([key, value]) => {
              if (value != undefined && value != null && value != ''){
                  arrayParams0.push({ key: key, value: value });
                }
              }
          );

    var param1 = JSON.stringify(arrayParams0.sort((a, b) => a.key > b.key ? 1 : -1));

    const arrayParams1: { key: string; value: any }[] = [];
          Object.entries((params[1])).forEach(([key, value]) => {
              if (value != undefined && value != null && value != ''){
                  arrayParams1.push({ key: key, value: value });
                }
              }
          );

    var param2 = JSON.stringify(arrayParams1.sort((a, b) => a.key > b.key ? 1 : -1));

    return param1 == param2;
  }

  compareMultipleStringValues(...params): boolean {
    const areAllStringEqual = arr => arr.every( v => v === arr[0] )
    return areAllStringEqual(params);
  }

  compareMultipleNumberValues(...params): boolean {
    var numberParamsToCompare : number[] = [];
    const areAllNumbersEqual = arr => arr.every( v => v === arr[0] )

      //Parse every value to Number
      params.forEach(parameter => {
        numberParamsToCompare.push(parseInt(parameter));
      });

      return areAllNumbersEqual(numberParamsToCompare);
  }

  compareMultipleFloatValues(...params): boolean {
    var floatParamsToCompare : number[] = [];
    const areAllDecimalsEqual = arr => arr.every( v => v === arr[0] )

      //Parse every value to Float
      params.forEach(parameter => {
        floatParamsToCompare.push(parseFloat(parameter));
      });
      
      return areAllDecimalsEqual(floatParamsToCompare);
  }

  amountIsLessThan1(...params): boolean {
    try {
      //Parse values
      var param1 = parseFloat(params[0].toString());
      var param2 = parseFloat(params[1].toString());

      return (param1 - param2) < 1;
    }
    catch(error){ //Error in Parsing to Float - Return False
      return false;
    }
  }
}
