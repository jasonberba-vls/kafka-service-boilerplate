export default class AutoMapper {
    AutoMap<T>(object: any, desination: T) : T {
        Object.keys(desination).forEach(keys=>{
            desination[keys] = object[keys];
        })
        return desination;
    }
}